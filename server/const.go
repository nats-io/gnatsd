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

package server

import (
	"regexp"
	"runtime/debug"
	"time"
)

// Command is a signal used to control a running nats-server process.
type Command string

// Valid Command values.
const (
	CommandStop   = Command("stop")
	CommandQuit   = Command("quit")
	CommandReopen = Command("reopen")
	CommandReload = Command("reload")

	// private for now
	commandLDMode = Command("ldm")
	commandTerm   = Command("term")
)

var (
	// gitCommit and serverVersion injected at build.
	gitCommit, serverVersion string
	// trustedKeys is a whitespace separated array of trusted operator's public nkeys.
	trustedKeys string
	// SemVer regexp to validate the VERSION.
	semVerRe = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)
)

// formatRevision formats a VCS revision string for display.
func formatRevision(revision string) string {
	if len(revision) >= 7 {
		return revision[:7]
	}
	return revision
}

func init() {
	// Use build info if present, it would be if building using 'go build .'
	// or when using a release.
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				gitCommit = formatRevision(setting.Value)
			}
		}
	}
}

const (
	// VERSION is the current version for the server.
	VERSION = "2.15.3-RC.2"

	// PROTO is the currently supported protocol.
	// 0 was the original
	// 1 maintains proto 0, adds echo abilities for CONNECT from the client. Clients
	// should not send echo unless proto in INFO is >= 1.
	PROTO = 1

	// DEFAULT_PORT is the default port for client connections.
	DEFAULT_PORT = 4222

	// RANDOM_PORT is the value for port that, when supplied, will cause the
	// server to listen on a randomly-chosen available port. The resolved port
	// is available via the Addr() method.
	RANDOM_PORT = -1

	// DEFAULT_HOST defaults to all interfaces.
	DEFAULT_HOST = "0.0.0.0"

	// MAX_CONTROL_LINE_SIZE is the maximum allowed protocol control line size.
	// 4k should be plenty since payloads sans connect/info string are separate.
	MAX_CONTROL_LINE_SIZE = 4096

	// MAX_PAYLOAD_SIZE is the maximum allowed payload size. Should be using
	// something different if > 1MB payloads are needed.
	MAX_PAYLOAD_SIZE = (1024 * 1024)

	// MAX_PAYLOAD_MAX_SIZE is the size at which the server will warn about
	// max_payload being too high. In the future, the server may enforce/reject
	// max_payload above this value.
	MAX_PAYLOAD_MAX_SIZE = (8 * 1024 * 1024)

	// MAX_PENDING_SIZE is the maximum outbound pending bytes per client.
	MAX_PENDING_SIZE = (64 * 1024 * 1024)

	// DEFAULT_MAX_CONNECTIONS is the default maximum connections allowed.
	DEFAULT_MAX_CONNECTIONS = (64 * 1024)

	// TLS_TIMEOUT is the TLS wait time.
	TLS_TIMEOUT = 2 * time.Second

	// DEFAULT_TLS_HANDSHAKE_FIRST_FALLBACK_DELAY is the default amount of
	// time for the server to wait for the TLS handshake with a client to
	// be initiated before falling back to sending the INFO protocol first.
	// See TLSHandshakeFirst and TLSHandshakeFirstFallback options.
	DEFAULT_TLS_HANDSHAKE_FIRST_FALLBACK_DELAY = 50 * time.Millisecond

	// AUTH_TIMEOUT is the authorization wait time.
	AUTH_TIMEOUT = 2 * time.Second

	// DEFAULT_PING_INTERVAL is how often pings are sent to clients, etc...
	DEFAULT_PING_INTERVAL = 2 * time.Minute

	// DEFAULT_PING_MAX_OUT is maximum allowed pings outstanding before disconnect.
	DEFAULT_PING_MAX_OUT = 2

	// CR_LF string
	CR_LF = "\r\n"

	// LEN_CR_LF hold onto the computed size.
	LEN_CR_LF = len(CR_LF)

	// DEFAULT_FLUSH_DEADLINE is the write/flush deadlines.
	DEFAULT_FLUSH_DEADLINE = 10 * time.Second

	// DEFAULT_HTTP_PORT is the default monitoring port.
	DEFAULT_HTTP_PORT = 8222

	// DEFAULT_HTTP_BASE_PATH is the default base path for monitoring.
	DEFAULT_HTTP_BASE_PATH = "/"

	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 10 * time.Millisecond

	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 1 * time.Second

	// DEFAULT_ROUTE_CONNECT Route solicitation intervals.
	DEFAULT_ROUTE_CONNECT = 1 * time.Second

	// DEFAULT_ROUTE_CONNECT_MAX Route solicitation intervals (max).
	DEFAULT_ROUTE_CONNECT_MAX = 30 * time.Second

	// DEFAULT_ROUTE_RECONNECT Route reconnect delay.
	DEFAULT_ROUTE_RECONNECT = 1 * time.Second

	// DEFAULT_ROUTE_DIAL Route dial timeout.
	DEFAULT_ROUTE_DIAL = 1 * time.Second

	// DEFAULT_ROUTE_POOL_SIZE Route default pool size
	DEFAULT_ROUTE_POOL_SIZE = 3

	// DEFAULT_LEAF_NODE_RECONNECT LeafNode reconnect interval.
	DEFAULT_LEAF_NODE_RECONNECT = time.Second

	// DEFAULT_LEAF_TLS_TIMEOUT TLS timeout for LeafNodes
	DEFAULT_LEAF_TLS_TIMEOUT = 2 * time.Second

	// PROTO_SNIPPET_SIZE is the default size of proto to print on parse errors.
	PROTO_SNIPPET_SIZE = 32

	// MAX_CONTROL_LINE_SNIPPET_SIZE is the default size of proto to print on max control line errors.
	MAX_CONTROL_LINE_SNIPPET_SIZE = 128

	// MAX_MSG_ARGS Maximum possible number of arguments from MSG proto.
	MAX_MSG_ARGS = 4

	// MAX_RMSG_ARGS Maximum possible number of arguments from RMSG proto.
	MAX_RMSG_ARGS = 6

	// MAX_HMSG_ARGS Maximum possible number of arguments from HMSG proto.
	MAX_HMSG_ARGS = 7

	// MAX_PUB_ARGS Maximum possible number of arguments from PUB proto.
	MAX_PUB_ARGS = 3

	// MAX_HPUB_ARGS Maximum possible number of arguments from HPUB proto.
	MAX_HPUB_ARGS = 4

	// MAX_RSUB_ARGS Maximum possible number of arguments from a RS+/LS+ proto.
	MAX_RSUB_ARGS = 6

	// DEFAULT_MAX_CLOSED_CLIENTS is the maximum number of closed connections we hold onto.
	DEFAULT_MAX_CLOSED_CLIENTS = 10000

	// DEFAULT_LAME_DUCK_DURATION is the time in which the server spreads
	// the closing of clients when signaled to go in lame duck mode.
	DEFAULT_LAME_DUCK_DURATION = 2 * time.Minute

	// DEFAULT_LAME_DUCK_GRACE_PERIOD is the duration the server waits, after entering
	// lame duck mode, before starting closing client connections.
	DEFAULT_LAME_DUCK_GRACE_PERIOD = 10 * time.Second

	// DEFAULT_LEAFNODE_INFO_WAIT Route dial timeout.
	DEFAULT_LEAFNODE_INFO_WAIT = 1 * time.Second

	// DEFAULT_LEAFNODE_PORT is the default port for remote leafnode connections.
	DEFAULT_LEAFNODE_PORT = 7422

	// DEFAULT_CONNECT_ERROR_REPORTS is the number of attempts at which a
	// repeated failed route, gateway or leaf node connection is reported.
	// This is used for initial connection, that is, when the server has
	// never had a connection to the given endpoint. Once connected, and
	// if a disconnect occurs, DEFAULT_RECONNECT_ERROR_REPORTS is used
	// instead.
	// The default is to report every 3600 attempts (roughly every hour).
	DEFAULT_CONNECT_ERROR_REPORTS = 3600

	// DEFAULT_RECONNECT_ERROR_REPORTS is the default number of failed
	// attempt to reconnect a route, gateway or leaf node connection.
	// The default is to report every attempt.
	DEFAULT_RECONNECT_ERROR_REPORTS = 1

	// DEFAULT_RTT_MEASUREMENT_INTERVAL is how often we want to measure RTT from
	// this server to clients, routes, gateways or leafnode connections.
	DEFAULT_RTT_MEASUREMENT_INTERVAL = time.Hour

	// DEFAULT_ALLOW_RESPONSE_MAX_MSGS is the default number of responses allowed
	// for a reply subject.
	DEFAULT_ALLOW_RESPONSE_MAX_MSGS = 1

	// DEFAULT_ALLOW_RESPONSE_EXPIRATION is the default time allowed for a given
	// dynamic response permission.
	DEFAULT_ALLOW_RESPONSE_EXPIRATION = 2 * time.Minute

	// DEFAULT_SERVICE_EXPORT_RESPONSE_THRESHOLD is the default time that the system will
	// expect a service export response to be delivered. This is used in corner cases for
	// time based cleanup of reverse mapping structures.
	DEFAULT_SERVICE_EXPORT_RESPONSE_THRESHOLD = 2 * time.Minute

	// DEFAULT_SERVICE_LATENCY_SAMPLING is the default sampling rate for service
	// latency metrics
	DEFAULT_SERVICE_LATENCY_SAMPLING = 100

	// DEFAULT_SYSTEM_ACCOUNT
	DEFAULT_SYSTEM_ACCOUNT = "$SYS"

	// DEFAULT GLOBAL_ACCOUNT
	DEFAULT_GLOBAL_ACCOUNT = "$G"

	// DEFAULT_FETCH_TIMEOUT is the default time that the system will wait for an account fetch to return.
	DEFAULT_ACCOUNT_FETCH_TIMEOUT = 1900 * time.Millisecond
)
