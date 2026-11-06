// Copyright 2024-2025 The NATS Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
)

const (
	// JSApiLevel is the maximum supported JetStream API level for this server.
	JSApiLevel int = 4

	JSRequiredLevelMetadataKey = "_nats.req.level"
	JSServerVersionMetadataKey = "_nats.ver"
	JSServerLevelMetadataKey   = "_nats.level"
)

// getRequiredApiLevel returns the required API level for the JetStream asset.
func getRequiredApiLevel(metadata map[string]string) string {
	if l, ok := metadata[JSRequiredLevelMetadataKey]; ok && l != _EMPTY_ {
		return l
	}
	return _EMPTY_
}

// supportsRequiredApiLevel returns whether the required API level for the JetStream asset is supported.
func supportsRequiredApiLevel(metadata map[string]string) bool {
	if l := getRequiredApiLevel(metadata); l != _EMPTY_ {
		li, err := strconv.Atoi(l)
		return err == nil && li <= JSApiLevel
	}
	return true
}

// errInvalidConfigValue is returned for a config value this server can't decode.
// This can be an enum value added in a newer version.
var errInvalidConfigValue = errors.New("invalid value")

// decodeConfig strictly decodes an asset config, and leniently decodes it if that fails.
// strictErr is set if the config holds fields or values this server doesn't support.
// The asset should then be marked as unsupported.
// err is only set if the config can't be decoded at all.
func decodeConfig[T any](buf []byte) (cfg T, strictErr, err error) {
	decoder := json.NewDecoder(bytes.NewReader(buf))
	decoder.DisallowUnknownFields()
	if strictErr = decoder.Decode(&cfg); strictErr == nil {
		return cfg, nil, nil
	}

	// The strict decode may have partially filled the config.
	var zero T
	cfg = zero
	if err = json.Unmarshal(buf, &cfg); err == nil {
		return cfg, strictErr, nil
	}

	// Decode field by field, so a field this server can't decode doesn't fail the others.
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(buf, &fields); err != nil {
		return zero, strictErr, err
	}
	// The lenient decode may have partially filled the config.
	cfg = zero
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		b, err := json.Marshal(map[string]json.RawMessage{k: fields[k]})
		if err != nil {
			continue
		}
		// Check the field decodes fully first.
		// A field that fails to decode must stay unset.
		var tmp T
		if err = json.Unmarshal(b, &tmp); err != nil {
			// The decode error doesn't name the field.
			// Report the first field that can't be decoded instead.
			if !errors.Is(strictErr, errInvalidConfigValue) {
				// Objects and arrays can be large.
				// Only include plain values.
				if v := fields[k]; len(v) > 0 && v[0] != '{' && v[0] != '[' {
					strictErr = fmt.Errorf("%w %s for field %q", errInvalidConfigValue, v, k)
				} else {
					strictErr = fmt.Errorf("%w for field %q", errInvalidConfigValue, k)
				}
			}
			continue
		}
		_ = json.Unmarshal(b, &cfg)
	}
	return cfg, strictErr, nil
}

// setStaticStreamMetadata sets JetStream stream metadata, like the server version and API level.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
func setStaticStreamMetadata(cfg *StreamConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	} else {
		deleteDynamicMetadata(cfg.Metadata)
	}

	var requiredApiLevel int
	requires := func(level int) {
		if level > requiredApiLevel {
			requiredApiLevel = level
		}
	}

	// TTLs were added in v2.11 and require API level 1.
	if cfg.AllowMsgTTL || cfg.SubjectDeleteMarkerTTL > 0 {
		requires(1)
	}

	// Counter CRDTs were added in v2.12 and require API level 2.
	if cfg.AllowMsgCounter {
		requires(2)
	}

	// Atomic batch publishing was added in v2.12 and require API level 2.
	if cfg.AllowAtomicPublish {
		requires(2)
	}

	// Message scheduling was added in v2.12 and require API level 2.
	if cfg.AllowMsgSchedules {
		requires(2)
	}

	// Async persist mode was added in v2.12 and requires API level 2.
	if cfg.PersistMode == AsyncPersistMode {
		requires(2)
	}

	// Fast batch publishing was added in v2.14 and requires API level 4.
	if cfg.AllowBatchPublish {
		requires(4)
	}

	cfg.Metadata[JSRequiredLevelMetadataKey] = strconv.Itoa(requiredApiLevel)
}

// setDynamicStreamMetadata adds dynamic fields into the (copied) metadata.
func setDynamicStreamMetadata(cfg *StreamConfig) *StreamConfig {
	var newCfg StreamConfig
	if cfg != nil {
		newCfg = *cfg
	}
	newCfg.Metadata = make(map[string]string)
	if cfg != nil {
		for key, value := range cfg.Metadata {
			newCfg.Metadata[key] = value
		}
	}
	newCfg.Metadata[JSServerVersionMetadataKey] = VERSION
	newCfg.Metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	return &newCfg
}

// copyConsumerMetadata copies versioning fields from metadata of prevCfg into cfg.
// Removes versioning fields if no previous metadata, updates if set, and removes fields if it doesn't exist in prevCfg.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
//
// Note: useful when doing equality checks on cfg and prevCfg, but ignoring any versioning metadata differences.
func copyStreamMetadata(cfg *StreamConfig, prevCfg *StreamConfig) {
	if cfg.Metadata != nil {
		deleteDynamicMetadata(cfg.Metadata)
	}
	setOrDeleteInStreamMetadata(cfg, prevCfg, JSRequiredLevelMetadataKey)
}

// setOrDeleteInConsumerMetadata sets field with key/value in metadata of cfg if set, deletes otherwise.
func setOrDeleteInStreamMetadata(cfg *StreamConfig, prevCfg *StreamConfig, key string) {
	if prevCfg != nil && prevCfg.Metadata != nil {
		if value, ok := prevCfg.Metadata[key]; ok {
			if cfg.Metadata == nil {
				cfg.Metadata = make(map[string]string)
			}
			cfg.Metadata[key] = value
			return
		}
	}
	delete(cfg.Metadata, key)
	if len(cfg.Metadata) == 0 {
		cfg.Metadata = nil
	}
}

// setStaticConsumerMetadata sets JetStream consumer metadata, like the server version and API level.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
func setStaticConsumerMetadata(cfg *ConsumerConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	} else {
		deleteDynamicMetadata(cfg.Metadata)
	}

	var requiredApiLevel int
	requires := func(level int) {
		if level > requiredApiLevel {
			requiredApiLevel = level
		}
	}

	// Added in 2.11, absent | zero is the feature is not used.
	// one could be stricter and say even if its set but the time
	// has already passed it is also not needed to restore the consumer
	if cfg.PauseUntil != nil && !cfg.PauseUntil.IsZero() {
		requires(1)
	}

	if cfg.PriorityPolicy != PriorityNone || cfg.PinnedTTL != 0 || len(cfg.PriorityGroups) > 0 {
		requires(1)
	}

	// Added in 2.14
	if cfg.AckPolicy == AckFlowControl {
		requires(4)
	}

	cfg.Metadata[JSRequiredLevelMetadataKey] = strconv.Itoa(requiredApiLevel)
}

// setDynamicConsumerMetadata adds dynamic fields into the (copied) metadata.
func setDynamicConsumerMetadata(cfg *ConsumerConfig) *ConsumerConfig {
	var newCfg ConsumerConfig
	if cfg != nil {
		newCfg = *cfg
	}
	newCfg.Metadata = make(map[string]string)
	if cfg != nil {
		for key, value := range cfg.Metadata {
			newCfg.Metadata[key] = value
		}
	}
	newCfg.Metadata[JSServerVersionMetadataKey] = VERSION
	newCfg.Metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	return &newCfg
}

// setDynamicConsumerInfoMetadata adds dynamic fields into the (copied) metadata.
func setDynamicConsumerInfoMetadata(info *ConsumerInfo) *ConsumerInfo {
	if info == nil {
		return nil
	}

	newInfo := *info
	cfg := setDynamicConsumerMetadata(info.Config)
	newInfo.Config = cfg
	return &newInfo
}

// copyConsumerMetadata copies versioning fields from metadata of prevCfg into cfg.
// Removes versioning fields if no previous metadata, updates if set, and removes fields if it doesn't exist in prevCfg.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
//
// Note: useful when doing equality checks on cfg and prevCfg, but ignoring any versioning metadata differences.
func copyConsumerMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig) {
	if cfg.Metadata != nil {
		deleteDynamicMetadata(cfg.Metadata)
	}
	setOrDeleteInConsumerMetadata(cfg, prevCfg, JSRequiredLevelMetadataKey)
}

// setOrDeleteInConsumerMetadata sets field with key/value in metadata of cfg if set, deletes otherwise.
func setOrDeleteInConsumerMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig, key string) {
	if prevCfg != nil && prevCfg.Metadata != nil {
		if value, ok := prevCfg.Metadata[key]; ok {
			if cfg.Metadata == nil {
				cfg.Metadata = make(map[string]string)
			}
			cfg.Metadata[key] = value
			return
		}
	}
	delete(cfg.Metadata, key)
	if len(cfg.Metadata) == 0 {
		cfg.Metadata = nil
	}
}

// deleteDynamicMetadata deletes dynamic fields from the metadata.
func deleteDynamicMetadata(metadata map[string]string) {
	delete(metadata, JSServerVersionMetadataKey)
	delete(metadata, JSServerLevelMetadataKey)
}

// errorOnRequiredApiLevel returns whether a request should be rejected based on the JSRequiredApiLevel header.
func errorOnRequiredApiLevel(hdr []byte) bool {
	reqApiLevel := sliceHeader(JSRequiredApiLevel, hdr)
	if len(reqApiLevel) == 0 {
		return false
	}
	minLevel, err := strconv.Atoi(string(reqApiLevel))
	return err != nil || JSApiLevel < minLevel
}
