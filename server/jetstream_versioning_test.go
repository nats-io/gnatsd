// Copyright 2024-2026 The NATS Authors
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
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
)

func TestGetAndSupportsRequiredApiLevel(t *testing.T) {
	require_Equal(t, getRequiredApiLevel(nil), _EMPTY_)
	require_Equal(t, getRequiredApiLevel(map[string]string{}), _EMPTY_)
	require_Equal(t, getRequiredApiLevel(map[string]string{JSRequiredLevelMetadataKey: "1"}), "1")
	require_Equal(t, getRequiredApiLevel(map[string]string{JSRequiredLevelMetadataKey: "text"}), "text")

	require_True(t, supportsRequiredApiLevel(nil))
	require_True(t, supportsRequiredApiLevel(map[string]string{}))
	require_True(t, supportsRequiredApiLevel(map[string]string{JSRequiredLevelMetadataKey: "1"}))
	require_True(t, supportsRequiredApiLevel(map[string]string{JSRequiredLevelMetadataKey: strconv.Itoa(JSApiLevel)}))
	require_False(t, supportsRequiredApiLevel(map[string]string{JSRequiredLevelMetadataKey: "text"}))
}

func metadataAtLevel(featureLevel string) map[string]string {
	return map[string]string{
		JSRequiredLevelMetadataKey: featureLevel,
	}
}

func metadataPrevious() map[string]string {
	return map[string]string{
		JSRequiredLevelMetadataKey: "previous-level",
	}
}

func TestJetStreamSetStaticStreamMetadata(t *testing.T) {
	for _, test := range []struct {
		desc             string
		cfg              *StreamConfig
		expectedMetadata map[string]string
	}{
		{
			desc:             "empty",
			cfg:              &StreamConfig{},
			expectedMetadata: metadataAtLevel("0"),
		},
		{
			desc:             "overwrite-user-provided",
			cfg:              &StreamConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataAtLevel("0"),
		},
		{
			desc:             "empty-prev-metadata/delete-user-provided",
			cfg:              &StreamConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataAtLevel("0"),
		},
		{
			desc:             "AllowMsgTTL",
			cfg:              &StreamConfig{AllowMsgTTL: true},
			expectedMetadata: metadataAtLevel("1"),
		},
		{
			desc:             "SubjectDeleteMarkerTTL",
			cfg:              &StreamConfig{SubjectDeleteMarkerTTL: time.Second},
			expectedMetadata: metadataAtLevel("1"),
		},
		{
			desc:             "AllowMsgCounter",
			cfg:              &StreamConfig{AllowMsgCounter: true},
			expectedMetadata: metadataAtLevel("2"),
		},
		{
			desc:             "AllowAtomicPublish",
			cfg:              &StreamConfig{AllowAtomicPublish: true},
			expectedMetadata: metadataAtLevel("2"),
		},
		{
			desc:             "AllowMsgSchedules",
			cfg:              &StreamConfig{AllowMsgSchedules: true},
			expectedMetadata: metadataAtLevel("2"),
		},
		{
			desc:             "AsyncPersistMode",
			cfg:              &StreamConfig{PersistMode: AsyncPersistMode},
			expectedMetadata: metadataAtLevel("2"),
		},
		{
			desc:             "AllowBatchPublish",
			cfg:              &StreamConfig{AllowBatchPublish: true},
			expectedMetadata: metadataAtLevel("4"),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			setStaticStreamMetadata(test.cfg)
			level := test.cfg.Metadata[JSRequiredLevelMetadataKey]
			require_Equal(t, level, test.expectedMetadata[JSRequiredLevelMetadataKey])

			// Ensure we up the server API level if we introduced a feature that requires it.
			l, err := strconv.Atoi(level)
			require_NoError(t, err)
			require_True(t, l <= JSApiLevel)
		})
	}
}

func TestJetStreamSetStaticStreamMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := StreamConfig{Metadata: dynamicMetadata()}
	setStaticStreamMetadata(&cfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAtLevel("0")))
}

func TestJetStreamSetDynamicStreamMetadata(t *testing.T) {
	cfg := StreamConfig{Metadata: metadataAtLevel("0")}
	newCfg := setDynamicStreamMetadata(&cfg)

	// Only new metadata must contain dynamic fields.
	metadata := metadataAtLevel("0")
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadata))
	metadata[JSServerVersionMetadataKey] = VERSION
	metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	require_True(t, reflect.DeepEqual(newCfg.Metadata, metadata))
}

func TestJetStreamCopyStreamMetadata(t *testing.T) {
	for _, test := range []struct {
		desc string
		cfg  *StreamConfig
		prev *StreamConfig
	}{
		{
			desc: "no-previous-ignore",
			cfg:  &StreamConfig{Metadata: metadataAtLevel("-1")},
			prev: nil,
		},
		{
			desc: "nil-previous-metadata-ignore",
			cfg:  &StreamConfig{Metadata: metadataAtLevel("-1")},
			prev: &StreamConfig{Metadata: nil},
		},
		{
			desc: "nil-current-metadata-ignore",
			cfg:  &StreamConfig{Metadata: nil},
			prev: &StreamConfig{Metadata: metadataPrevious()},
		},
		{
			desc: "copy-previous",
			cfg:  &StreamConfig{Metadata: metadataAtLevel("-1")},
			prev: &StreamConfig{Metadata: metadataPrevious()},
		},
		{
			desc: "delete-missing-fields",
			cfg:  &StreamConfig{Metadata: metadataAtLevel("-1")},
			prev: &StreamConfig{Metadata: make(map[string]string)},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			copyStreamMetadata(test.cfg, test.prev)

			var expectedMetadata map[string]string
			if test.prev != nil {
				expectedMetadata = test.prev.Metadata
			}

			value, ok := expectedMetadata[JSRequiredLevelMetadataKey]
			if ok {
				require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], value)
			} else {
				// Key shouldn't exist.
				_, ok = test.cfg.Metadata[JSRequiredLevelMetadataKey]
				require_False(t, ok)
			}
		})
	}
}

func TestJetStreamCopyStreamMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := StreamConfig{Metadata: dynamicMetadata()}
	copyStreamMetadata(&cfg, nil)
	require_Equal(t, len(cfg.Metadata), 0)

	cfg = StreamConfig{Metadata: dynamicMetadata()}
	prevCfg := StreamConfig{Metadata: metadataAtLevel("0")}
	copyStreamMetadata(&cfg, &prevCfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAtLevel("0")))
}

func TestJetStreamSetStaticConsumerMetadata(t *testing.T) {
	pauseUntil := time.Unix(0, 0)
	pauseUntilZero := time.Time{}
	for _, test := range []struct {
		desc             string
		cfg              *ConsumerConfig
		expectedMetadata map[string]string
	}{
		{
			desc:             "empty",
			cfg:              &ConsumerConfig{},
			expectedMetadata: metadataAtLevel("0"),
		},
		{
			desc:             "overwrite-user-provided",
			cfg:              &ConsumerConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataAtLevel("0"),
		},
		{
			desc:             "PauseUntil/zero",
			cfg:              &ConsumerConfig{PauseUntil: &pauseUntilZero},
			expectedMetadata: metadataAtLevel("0"),
		},
		{
			desc:             "PauseUntil",
			cfg:              &ConsumerConfig{PauseUntil: &pauseUntil},
			expectedMetadata: metadataAtLevel("1"),
		},
		{
			desc:             "Pinned",
			cfg:              &ConsumerConfig{PriorityPolicy: PriorityPinnedClient, PriorityGroups: []string{"a"}},
			expectedMetadata: metadataAtLevel("1"),
		},
		{
			desc:             "AckFlowControl",
			cfg:              &ConsumerConfig{AckPolicy: AckFlowControl},
			expectedMetadata: metadataAtLevel("4"),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			setStaticConsumerMetadata(test.cfg)
			level := test.cfg.Metadata[JSRequiredLevelMetadataKey]
			require_Equal(t, level, test.expectedMetadata[JSRequiredLevelMetadataKey])

			// Ensure we up the server API level if we introduced a feature that requires it.
			l, err := strconv.Atoi(level)
			require_NoError(t, err)
			require_True(t, l <= JSApiLevel)
		})
	}
}

func TestJetStreamSetStaticConsumerMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := ConsumerConfig{Metadata: dynamicMetadata()}
	setStaticConsumerMetadata(&cfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAtLevel("0")))
}

func TestJetStreamSetDynamicConsumerMetadata(t *testing.T) {
	cfg := ConsumerConfig{Metadata: metadataAtLevel("0")}
	newCfg := setDynamicConsumerMetadata(&cfg)

	// Only new metadata must contain dynamic fields.
	metadata := metadataAtLevel("0")
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadata))
	metadata[JSServerVersionMetadataKey] = VERSION
	metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	require_True(t, reflect.DeepEqual(newCfg.Metadata, metadata))
}

func TestJetStreamSetDynamicConsumerInfoMetadata(t *testing.T) {
	ci := ConsumerInfo{Config: &ConsumerConfig{Metadata: metadataAtLevel("0")}}
	newCi := setDynamicConsumerInfoMetadata(&ci)

	// Configs should not equal, as that would mean we've overwritten the original ConsumerInfo.
	require_False(t, reflect.DeepEqual(ci, newCi))

	// Only new metadata must contain dynamic fields.
	metadata := metadataAtLevel("0")
	require_True(t, reflect.DeepEqual(ci.Config.Metadata, metadata))
	metadata[JSServerVersionMetadataKey] = VERSION
	metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	require_True(t, reflect.DeepEqual(newCi.Config.Metadata, metadata))
}

func TestJetStreamCopyConsumerMetadata(t *testing.T) {
	for _, test := range []struct {
		desc string
		cfg  *ConsumerConfig
		prev *ConsumerConfig
	}{
		{
			desc: "no-previous-ignore",
			cfg:  &ConsumerConfig{Metadata: metadataAtLevel("-1")},
			prev: nil,
		},
		{
			desc: "nil-previous-metadata-ignore",
			cfg:  &ConsumerConfig{Metadata: metadataAtLevel("-1")},
			prev: &ConsumerConfig{Metadata: nil},
		},
		{
			desc: "nil-current-metadata-ignore",
			cfg:  &ConsumerConfig{Metadata: nil},
			prev: &ConsumerConfig{Metadata: metadataPrevious()},
		},
		{
			desc: "copy-previous",
			cfg:  &ConsumerConfig{Metadata: metadataAtLevel("-1")},
			prev: &ConsumerConfig{Metadata: metadataPrevious()},
		},
		{
			desc: "delete-missing-fields",
			cfg:  &ConsumerConfig{Metadata: metadataAtLevel("-1")},
			prev: &ConsumerConfig{Metadata: make(map[string]string)},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			copyConsumerMetadata(test.cfg, test.prev)

			var expectedMetadata map[string]string
			if test.prev != nil {
				expectedMetadata = test.prev.Metadata
			}

			value, ok := expectedMetadata[JSRequiredLevelMetadataKey]
			if ok {
				require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], value)
			} else {
				// Key shouldn't exist.
				_, ok = test.cfg.Metadata[JSRequiredLevelMetadataKey]
				require_False(t, ok)
			}
		})
	}
}

func TestJetStreamCopyConsumerMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := ConsumerConfig{Metadata: dynamicMetadata()}
	copyConsumerMetadata(&cfg, nil)
	require_Equal(t, len(cfg.Metadata), 0)

	cfg = ConsumerConfig{Metadata: dynamicMetadata()}
	prevCfg := ConsumerConfig{Metadata: metadataAtLevel("0")}
	copyConsumerMetadata(&cfg, &prevCfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAtLevel("0")))
}

type server struct {
	replicas int
	js       nats.JetStreamContext
	nc       *nats.Conn
}

const (
	streamName   = "STREAM"
	consumerName = "CONSUMER"
)

func TestJetStreamMetadataMutations(t *testing.T) {
	single := RunBasicJetStreamServer(t)
	defer single.Shutdown()
	nc, js := jsClientConnect(t, single)
	defer nc.Close()

	cluster := createJetStreamClusterExplicit(t, "R3S", 3)
	defer cluster.shutdown()
	cnc, cjs := jsClientConnect(t, cluster.randomServer())
	defer cnc.Close()

	// Test for both single server and clustered mode.
	for _, s := range []server{
		{1, js, nc},
		{3, cjs, cnc},
	} {
		t.Run(fmt.Sprintf("R%d", s.replicas), func(t *testing.T) {
			streamMetadataChecks(t, s)
			consumerMetadataChecks(t, s)
		})
	}
}

func validateMetadata(metadata map[string]string, expectedFeatureLevel string) bool {
	return metadata[JSRequiredLevelMetadataKey] == expectedFeatureLevel ||
		metadata[JSServerVersionMetadataKey] == VERSION ||
		metadata[JSServerLevelMetadataKey] == strconv.Itoa(JSApiLevel)
}

func streamMetadataChecks(t *testing.T, s server) {
	// Add stream.
	sc := nats.StreamConfig{Name: streamName, Replicas: s.replicas}
	si, err := s.js.AddStream(&sc)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))

	// (Double) add stream, has different code path for clustered streams.
	sc = nats.StreamConfig{Name: streamName, Replicas: s.replicas}
	si, err = s.js.AddStream(&sc)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))

	// Stream info.
	si, err = s.js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))

	// Update stream.
	// Metadata set on creation should be preserved, even if not included in update.
	si, err = s.js.UpdateStream(&sc)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))
}

func consumerMetadataChecks(t *testing.T, s server) {
	// Add consumer.
	cc := nats.ConsumerConfig{Name: consumerName, Replicas: s.replicas}
	ci, err := s.js.AddConsumer(streamName, &cc)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Consumer info.
	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Update consumer.
	// Metadata set on creation should be preserved, even if not included in update.
	ci, err = s.js.UpdateConsumer(streamName, &cc)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Use pause advisories to know when pause/resume is applied.
	pauseCh := make(chan *nats.Msg, 10)
	_, err = s.nc.ChanSubscribe(JSAdvisoryConsumerPausePre+".STREAM.CONSUMER", pauseCh)
	require_NoError(t, err)

	// Pause consumer, should up required API level.
	jsTestPause_PauseConsumer(t, s.nc, streamName, consumerName, time.Now().Add(time.Second*3))
	require_ChanRead(t, pauseCh, time.Second*2)
	require_Len(t, len(pauseCh), 0)

	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "1"))

	// Unpause consumer, should lower required API level.
	subj := fmt.Sprintf("$JS.API.CONSUMER.PAUSE.%s.%s", streamName, consumerName)
	_, err = s.nc.Request(subj, nil, time.Second)
	require_NoError(t, err)
	require_ChanRead(t, pauseCh, time.Second*2)
	require_Len(t, len(pauseCh), 0)

	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Test scaling up/down.
	if s.replicas == 3 {
		// Scale down.
		cc.Replicas = 1
		ci, err = s.js.UpdateConsumer(streamName, &cc)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))

		ci, err = s.js.ConsumerInfo(streamName, consumerName)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))

		// Scale up.
		cc.Replicas = 3
		ci, err = s.js.UpdateConsumer(streamName, &cc)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))

		ci, err = s.js.ConsumerInfo(streamName, consumerName)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))
	}
}

func TestJetStreamMetadataStreamRestoreAndRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	restoreEmptyStream(t, nc, 1)

	expectedMetadata := map[string]string{
		JSServerVersionMetadataKey: VERSION,
		JSServerLevelMetadataKey:   strconv.Itoa(JSApiLevel),
	}

	// Stream restore should result in empty metadata to be preserved, only adding dynamic metadata.
	si, err := js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))

	// Restart server.
	port := s.opts.Port
	sd := s.StoreDir()
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// After restart (or upgrade) metadata data should remain empty, only adding dynamic metadata.
	si, err = js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))
}

func TestJetStreamMetadataStreamRestoreAndRestartCluster(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	restoreEmptyStream(t, nc, 3)

	expectedMetadata := map[string]string{
		JSServerVersionMetadataKey: VERSION,
		JSServerLevelMetadataKey:   strconv.Itoa(JSApiLevel),
	}

	// Stream restore should result in empty metadata to be preserved, only adding dynamic metadata.
	si, err := js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))

	// Restart cluster.
	c.stopAll()
	c.restartAllSamePorts()
	defer c.shutdown()
	c.waitOnAllCurrent()
	c.waitOnStreamLeader("$G", streamName)
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// After restart (or upgrade) metadata data should remain empty, only adding dynamic metadata.
	si, err = js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))
}

func restoreEmptyStream(t *testing.T, nc *nats.Conn, replicas int) {
	rreq := JSApiStreamRestoreRequest{
		Config: StreamConfig{
			Name:      "STREAM",
			Retention: LimitsPolicy,
			Storage:   FileStorage,
			Replicas:  replicas,
		},
	}
	buf, err := json.Marshal(rreq)
	require_NoError(t, err)

	var rresp JSApiStreamRestoreResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, rreq.Config.Name), buf, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	// Construct empty stream.tar.s2 (only containing meta.inf).
	fsi := FileStreamInfo{StreamConfig: rreq.Config}
	fsij, err := json.Marshal(fsi)
	require_NoError(t, err)

	hdr := &tar.Header{
		Name:   JetStreamMetaFile,
		Mode:   0600,
		Uname:  "nats",
		Gname:  "nats",
		Size:   int64(len(fsij)),
		Format: tar.FormatPAX,
	}
	var buffer bytes.Buffer
	enc := s2.NewWriter(&buffer)
	tw := tar.NewWriter(enc)
	err = tw.WriteHeader(hdr)
	require_NoError(t, err)
	_, err = tw.Write(fsij)
	require_NoError(t, err)
	err = tw.Close()
	require_NoError(t, err)
	err = enc.Close()
	require_NoError(t, err)

	data := buffer.Bytes()
	msg, err = nc.Request(rresp.DeliverSubject, data, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	msg, err = nc.Request(rresp.DeliverSubject, nil, 5*time.Second)
	require_NoError(t, err)

	expectedMetadata := map[string]string{
		JSServerVersionMetadataKey: VERSION,
		JSServerLevelMetadataKey:   strconv.Itoa(JSApiLevel),
	}

	var cresp JSApiStreamCreateResponse
	err = json.Unmarshal(msg.Data, &cresp)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(cresp.Config.Metadata, expectedMetadata))
}

func TestJetStreamApiErrorOnRequiredApiLevel(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc := clientConnectToServer(t, s)
	defer nc.Close()

	var subs []*subscription
	s.getJetStream().apiSubs.All(&subs)
	require_True(t, len(subs) > 0)
	for _, sub := range subs {
		apiSubject := string(sub.subject)
		t.Run(apiSubject, func(t *testing.T) {
			req := nats.NewMsg(apiSubject)
			req.Header.Set("Nats-Required-Api-Level", strconv.Itoa(math.MaxInt))
			msg, err := nc.RequestMsg(req, time.Second)
			require_NoError(t, err)

			var resp ApiResponse
			require_NoError(t, json.Unmarshal(msg.Data, &resp))
			require_True(t, resp.Error != nil)
			// Peer remove, stepdown or cancel move is not supported if not clustered.
			if strings.Contains(apiSubject, ".STEPDOWN.") ||
				strings.Contains(apiSubject, ".PEER.") ||
				strings.Contains(apiSubject, ".CANCEL_MOVE.") {
				require_Error(t, resp.Error, NewJSClusterRequiredError())
			} else {
				require_Error(t, resp.Error, NewJSRequiredApiLevelError())
			}
		})
	}
}

func TestJetStreamApiErrorOnRequiredApiLevelDirectGet(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	req := nats.NewMsg(fmt.Sprintf(JSDirectMsgGetT, "TEST"))
	req.Header.Set("Nats-Required-Api-Level", strconv.Itoa(math.MaxInt))
	msg, err := nc.RequestMsg(req, time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get("Status"), "412")
	require_Equal(t, msg.Header.Get("Description"), "Required Api Level")

	req = nats.NewMsg(fmt.Sprintf(JSDirectGetLastBySubjectT, "TEST", "foo"))
	req.Header.Set("Nats-Required-Api-Level", strconv.Itoa(math.MaxInt))
	msg, err = nc.RequestMsg(req, time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get("Status"), "412")
	require_Equal(t, msg.Header.Get("Description"), "Required Api Level")
}

func TestJetStreamApiErrorOnRequiredApiLevelPullConsumerNextMsg(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "TEST",
		Subjects:    []string{"foo"},
		AllowDirect: true,
	})
	require_NoError(t, err)

	_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{Durable: "CONSUMER"})
	require_NoError(t, err)

	req := nats.NewMsg(fmt.Sprintf(JSApiRequestNextT, "TEST", "CONSUMER"))
	req.Header.Set("Nats-Required-Api-Level", strconv.Itoa(math.MaxInt))
	msg, err := nc.RequestMsg(req, time.Second)
	require_NoError(t, err)
	require_Equal(t, msg.Header.Get("Status"), "412")
	require_Equal(t, msg.Header.Get("Description"), "Required Api Level")
}

func TestJetStreamUnsupportedAssignmentWithUnknownEnum(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	cbuf := []byte(`{"name":"C","stream":"S","consumer":{"durable_name":"C","ack_policy":"unknown","max_deliver":-1,"metadata":{"_nats.req.level":"1000"}},"group":{"name":"G","peers":["A"]}}`)
	ca, err := decodeConsumerAssignment(cbuf)
	require_NoError(t, err)
	require_NotNil(t, ca.unsupported)
	require_Equal(t, ca.unsupported.reason, `unsupported - config error: invalid value "unknown" for field "ack_policy"`)
	require_Equal(t, ca.Config.Durable, "C")
	require_Equal(t, ca.Config.MaxDeliver, -1)
	require_Equal(t, getRequiredApiLevel(ca.Config.Metadata), "1000")

	sbuf := []byte(`{"stream":{"name":"S","retention":"unknown","max_msgs":10,"metadata":{"_nats.req.level":"1000"}},"group":{"name":"G","peers":["A"]}}`)
	sa, err := decodeStreamAssignment(s, sbuf)
	require_NoError(t, err)
	require_NotNil(t, sa.unsupported)
	require_Equal(t, sa.unsupported.reason, `unsupported - config error: invalid value "unknown" for field "retention"`)
	require_Equal(t, sa.Config.Name, "S")
	require_Equal(t, sa.Config.MaxMsgs, 10)
	require_Equal(t, getRequiredApiLevel(sa.Config.Metadata), "1000")

	// A field that fails to decode is left unset, even if part of it decoded.
	sbuf = []byte(`{"stream":{"name":"S","mirror":{"name":"M","opt_start_seq":"x"}},"group":{"name":"G","peers":["A"]}}`)
	sa, err = decodeStreamAssignment(s, sbuf)
	require_NoError(t, err)
	require_NotNil(t, sa.unsupported)
	require_Equal(t, sa.unsupported.reason, `unsupported - config error: invalid value for field "mirror"`)
	require_Equal(t, sa.Config.Name, "S")
	require_True(t, sa.Config.Mirror == nil)

	// An unknown storage type falls back to file storage, so the config can still be marshaled.
	sbuf = []byte(`{"stream":{"name":"S","storage":"unknown"},"group":{"name":"G","peers":["A"]}}`)
	sa, err = decodeStreamAssignment(s, sbuf)
	require_NoError(t, err)
	require_NotNil(t, sa.unsupported)
	require_Equal(t, sa.unsupported.reason, `unsupported - config error: invalid value "unknown" for field "storage"`)
	require_Equal(t, sa.Config.Storage, FileStorage)
	_, err = json.Marshal(sa.Config)
	require_NoError(t, err)

	// A config that isn't an object still fails to decode.
	_, err = decodeConsumerAssignment([]byte(`{"name":"C","stream":"S","consumer":"invalid"}`))
	require_Error(t, err)
}

func TestJetStreamUnsupportedAssignmentProposalKeepsConfig(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	scfg := `{"name":"S","storage":"unknown","unknown":true}`
	sa, err := decodeStreamAssignment(s, fmt.Appendf(nil, `{"stream":%s,"group":{"name":"G","peers":["A"],"store":"file"}}`, scfg))
	require_NoError(t, err)
	require_NotNil(t, sa.unsupported)
	for _, buf := range [][]byte{encodeAddStreamAssignment(sa), encodeUpdateStreamAssignment(sa), encodeDeleteStreamAssignment(sa)} {
		nsa, err := decodeStreamAssignment(s, buf[1:])
		require_NoError(t, err)
		require_Equal(t, string(nsa.ConfigJSON), scfg)
	}

	ccfg := `{"durable_name":"C","ack_policy":"unknown","unknown":true}`
	ca, err := decodeConsumerAssignment(fmt.Appendf(nil, `{"name":"C","stream":"S","consumer":%s,"group":{"name":"G","peers":["A"],"store":"file"}}`, ccfg))
	require_NoError(t, err)
	require_NotNil(t, ca.unsupported)
	for _, buf := range [][]byte{encodeAddConsumerAssignment(ca), encodeDeleteConsumerAssignment(ca)} {
		nca, err := decodeConsumerAssignment(buf[1:])
		require_NoError(t, err)
		require_Equal(t, string(nca.ConfigJSON), ccfg)
	}
	nca, err := decodeConsumerAssignmentCompressed(encodeAddConsumerAssignmentCompressed(ca)[1:])
	require_NoError(t, err)
	require_Equal(t, string(nca.ConfigJSON), ccfg)
}
