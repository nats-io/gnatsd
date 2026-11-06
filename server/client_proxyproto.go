// Copyright 2025-2026 The NATS Authors
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PROXY protocol v2 constants
const (
	// Protocol signature (12 bytes)
	proxyProtoV2Sig = "\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"

	// Version and command byte format: version(4 bits) | command(4 bits)
	proxyProtoV2VerMask = 0xF0
	proxyProtoV2Ver     = 0x20 // Version 2

	// Commands
	proxyProtoCmdMask  = 0x0F
	proxyProtoCmdLocal = 0x00 // LOCAL command (health check, use original connection)
	proxyProtoCmdProxy = 0x01 // PROXY command (proxied connection)

	// Address family and protocol byte format: family(4 bits) | protocol(4 bits)
	proxyProtoFamilyMask    = 0xF0
	proxyProtoFamilyUnspec  = 0x00 // Unspecified
	proxyProtoFamilyInet    = 0x10 // IPv4
	proxyProtoFamilyInet6   = 0x20 // IPv6
	proxyProtoFamilyUnix    = 0x30 // Unix socket
	proxyProtoProtoMask     = 0x0F
	proxyProtoProtoUnspec   = 0x00 // Unspecified
	proxyProtoProtoStream   = 0x01 // TCP/STREAM
	proxyProtoProtoDatagram = 0x02 // UDP/DGRAM

	// Address sizes
	proxyProtoAddrSizeIPv4 = 12 // 4 (src IP) + 4 (dst IP) + 2 (src port) + 2 (dst port)
	proxyProtoAddrSizeIPv6 = 36 // 16 (src IP) + 16 (dst IP) + 2 (src port) + 2 (dst port)

	// Header sizes
	proxyProtoV2HeaderSize = 16 // Fixed header: 12 (sig) + 1 (ver/cmd) + 1 (fam/proto) + 2 (addr len)

	// Timeout for reading PROXY protocol header
	proxyProtoReadTimeout = 5 * time.Second
)

// PROXY protocol v1 constants
const (
	proxyProtoV1Prefix     = "PROXY "
	proxyProtoV1MaxLineLen = 107 // Maximum line length including CRLF
	proxyProtoV1TCP4       = "TCP4"
	proxyProtoV1TCP6       = "TCP6"
	proxyProtoV1Unknown    = "UNKNOWN"
)

var (
	// Errors
	errProxyProtoInvalid      = errors.New("invalid PROXY protocol header")
	errProxyProtoUnsupported  = errors.New("unsupported PROXY protocol feature")
	errProxyProtoTimeout      = errors.New("timeout reading PROXY protocol header")
	errProxyProtoUnrecognized = errors.New("unrecognized PROXY protocol format")
)

// proxyProtoAddr contains the address information extracted from PROXY protocol header
type proxyProtoAddr struct {
	srcIP   net.IP
	srcPort uint16
	dstIP   net.IP
	dstPort uint16
}

// String implements net.Addr interface
func (p *proxyProtoAddr) String() string {
	return net.JoinHostPort(p.srcIP.String(), fmt.Sprintf("%d", p.srcPort))
}

// Network implements net.Addr interface
func (p *proxyProtoAddr) Network() string {
	if p.srcIP.To4() != nil {
		return "tcp4"
	}
	return "tcp6"
}

// proxyConn wraps a net.Conn to override RemoteAddr() with the address
// extracted from the PROXY protocol header
type proxyConn struct {
	net.Conn
	remoteAddr net.Addr
}

// RemoteAddr returns the original client address extracted from PROXY protocol
func (pc *proxyConn) RemoteAddr() net.Addr {
	return pc.remoteAddr
}

// proxyProtoListener wraps a net.Listener so that accepted connections
// consume an optional PROXY protocol header before any other data is read.
// It is used for listeners served by an http.Server (websocket), where the
// connection is handed to the HTTP (and possibly TLS) layer as soon as it is
// accepted, so the header cannot be read the way createClientEx() does it.
type proxyProtoListener struct {
	net.Listener
	s *Server
}

// Accept does not read the PROXY protocol header, since that would block the
// accept loop of the http.Server. The header is read by the returned
// connection the first time it is used, which happens in the goroutine
// that the http.Server dedicates to that connection.
func (l *proxyProtoListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &proxyProtoConn{Conn: conn, s: l.s}, nil
}

// proxyProtoConn reads an optional PROXY protocol header on first use. If a
// header with a PROXY command is found, RemoteAddr() returns the address that
// it carries. If no header is found, the bytes read during detection are
// replayed to the next layer and the connection is used as-is.
type proxyProtoConn struct {
	net.Conn
	s          *Server
	once       sync.Once
	remoteAddr net.Addr
	pre        []byte
	err        error

	mu  sync.Mutex
	rdl time.Time // Read deadline set by the upper layer, restored after the header is read.
}

func (c *proxyProtoConn) readHeader() {
	addr, pre, err := readProxyProtoHeader(c.Conn)
	// readProxyProtoHeader() cleared the read deadline, put back the one
	// that may have been set by the upper layer before we got here.
	c.mu.Lock()
	c.Conn.SetReadDeadline(c.rdl)
	c.mu.Unlock()
	if err != nil && err != errProxyProtoUnrecognized {
		// Do not warn for connections closed before sending anything,
		// which is what load balancer TCP health checks do.
		if !errors.Is(err, io.EOF) {
			c.s.Warnf("Error reading PROXY protocol header from %s: %v", c.Conn.RemoteAddr(), err)
		}
		c.err = err
		c.Conn.Close()
		return
	}
	// On errProxyProtoUnrecognized, pre holds the bytes consumed while
	// trying to detect the header. If addr is nil, it was a LOCAL/UNKNOWN
	// command (health check), so we keep the original remote address.
	c.pre = pre
	if addr != nil {
		c.remoteAddr = addr
	}
}

// Read implements net.Conn. It replays the bytes read past the header (if
// any) before reading from the underlying connection.
func (c *proxyProtoConn) Read(b []byte) (int, error) {
	c.once.Do(c.readHeader)
	if c.err != nil {
		return 0, c.err
	}
	if len(c.pre) > 0 {
		n := copy(b, c.pre)
		c.pre = c.pre[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

// RemoteAddr implements net.Conn. It returns the address extracted from the
// PROXY protocol header, if any, otherwise the address of the peer.
func (c *proxyProtoConn) RemoteAddr() net.Addr {
	c.once.Do(c.readHeader)
	if c.remoteAddr != nil {
		return c.remoteAddr
	}
	return c.Conn.RemoteAddr()
}

// SetDeadline implements net.Conn.
func (c *proxyProtoConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdl = t
	return c.Conn.SetDeadline(t)
}

// SetReadDeadline implements net.Conn.
func (c *proxyProtoConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdl = t
	return c.Conn.SetReadDeadline(t)
}

// detectProxyProtoVersion reads the first bytes and determines protocol version.
// Returns 1 for v1, 2 for v2, or error.
// The first 6 bytes read are returned so they can be used by the parser.
func detectProxyProtoVersion(conn net.Conn) (version int, header []byte, err error) {
	// Read first 6 bytes to check for "PROXY " or v2 signature
	header = make([]byte, 6)
	if _, err = io.ReadFull(conn, header); err != nil {
		return 0, nil, fmt.Errorf("failed to read protocol version: %w", err)
	}
	switch bytesToString(header) {
	case proxyProtoV1Prefix:
		return 1, header, nil
	case proxyProtoV2Sig[:6]:
		return 2, header, nil
	default:
		// Return the consumed bytes so the caller can replay them into the
		// next protocol layer instead of discarding them.
		return 0, header, errProxyProtoUnrecognized
	}
}

// readProxyProtoV1Header parses PROXY protocol v1 text format.
// Expects the "PROXY " prefix (6 bytes) to have already been consumed.
// Returns any bytes that were read past the trailing CRLF so the caller can
// replay them into the next protocol layer.
func readProxyProtoV1Header(conn net.Conn) (*proxyProtoAddr, []byte, error) {
	// Read rest of line (max 107 bytes total, already read 6)
	maxRemaining := proxyProtoV1MaxLineLen - 6

	// Read up to maxRemaining bytes at once (more efficient than byte-by-byte)
	buf := make([]byte, maxRemaining)
	var line []byte
	var remaining []byte

	for len(line) < maxRemaining {
		// Read available data
		n, err := conn.Read(buf[len(line):])
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read v1 line: %w", err)
		}

		line = buf[:len(line)+n]

		// Look for CRLF in what we've read so far
		for i := 0; i < len(line)-1; i++ {
			if line[i] == '\r' && line[i+1] == '\n' {
				// Found CRLF - keep any over-read bytes for the client parser.
				remaining = append(remaining, line[i+2:]...)
				line = line[:i]
				goto foundCRLF
			}
		}
	}

	// Exceeded max length without finding CRLF
	return nil, nil, fmt.Errorf("%w: v1 line too long", errProxyProtoInvalid)

foundCRLF:
	// Get parts from the protocol
	parts := strings.Fields(string(line))

	// Validate format
	if len(parts) < 1 {
		return nil, nil, fmt.Errorf("%w: invalid v1 format", errProxyProtoInvalid)
	}

	// Handle UNKNOWN (health check, like v2 LOCAL)
	if parts[0] == proxyProtoV1Unknown {
		return nil, remaining, nil
	}

	// Must have exactly 5 parts: protocol, src-ip, dst-ip, src-port, dst-port
	if len(parts) != 5 {
		return nil, nil, fmt.Errorf("%w: invalid v1 format", errProxyProtoInvalid)
	}

	protocol := parts[0]
	srcIP := net.ParseIP(parts[1])
	dstIP := net.ParseIP(parts[2])

	if srcIP == nil || dstIP == nil {
		return nil, nil, fmt.Errorf("%w: invalid address", errProxyProtoInvalid)
	}

	// Parse ports
	srcPort, err := strconv.ParseUint(parts[3], 10, 16)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid source port: %w", err)
	}

	dstPort, err := strconv.ParseUint(parts[4], 10, 16)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid dest port: %w", err)
	}

	// Validate protocol matches IP version. The textual form determines the
	// family: TCP4 requires dotted-quad addresses, TCP6 requires IPv6
	// addresses. IPv4-mapped IPv6 addresses (e.g. "::ffff:192.0.2.1") are
	// valid for TCP6 since dual-stack proxies can emit those for IPv4
	// clients on IPv6 sockets, matching the v2 parser behavior.
	srcIsV6 := strings.Contains(parts[1], ":")
	dstIsV6 := strings.Contains(parts[2], ":")
	if protocol == proxyProtoV1TCP4 && (srcIsV6 || dstIsV6) {
		return nil, nil, fmt.Errorf("%w: TCP4 with IPv6 address", errProxyProtoInvalid)
	}
	if protocol == proxyProtoV1TCP6 && (!srcIsV6 || !dstIsV6) {
		return nil, nil, fmt.Errorf("%w: TCP6 with IPv4 address", errProxyProtoInvalid)
	}
	if protocol != proxyProtoV1TCP4 && protocol != proxyProtoV1TCP6 {
		return nil, nil, fmt.Errorf("%w: invalid protocol %s", errProxyProtoInvalid, protocol)
	}

	return &proxyProtoAddr{
		srcIP:   srcIP,
		srcPort: uint16(srcPort),
		dstIP:   dstIP,
		dstPort: uint16(dstPort),
	}, remaining, nil
}

// readProxyProtoHeader reads and parses PROXY protocol (v1 or v2) from the connection.
// Automatically detects version and routes to appropriate parser.
// If the command is LOCAL/UNKNOWN (health check), it returns nil for addr and no error.
// If the command is PROXY, it returns the parsed address information.
// It also returns any bytes that were read past the v1 header terminator so the
// caller can replay them into the normal client parser.
// The connection must be fresh (no data read yet).
func readProxyProtoHeader(conn net.Conn) (*proxyProtoAddr, []byte, error) {
	// Set read deadline to prevent hanging on slow/malicious clients
	if err := conn.SetReadDeadline(time.Now().Add(proxyProtoReadTimeout)); err != nil {
		return nil, nil, err
	}
	defer conn.SetReadDeadline(time.Time{})

	// Detect version.
	// On errProxyProtoUnrecognized, firstBytes holds the bytes that were
	// consumed so the caller can replay them.
	version, firstBytes, err := detectProxyProtoVersion(conn)
	if err != nil {
		return nil, firstBytes, err
	}

	switch version {
	case 1:
		// v1 parser expects "PROXY " prefix already consumed
		return readProxyProtoV1Header(conn)
	case 2:
		// Read rest of v2 signature (bytes 6-11, total 6 more bytes)
		remaining := make([]byte, 6)
		if _, err := io.ReadFull(conn, remaining); err != nil {
			return nil, nil, fmt.Errorf("failed to read v2 signature: %w", err)
		}

		// Verify full signature
		fullSig := string(firstBytes) + string(remaining)
		if fullSig != proxyProtoV2Sig {
			return nil, nil, fmt.Errorf("%w: invalid signature", errProxyProtoInvalid)
		}

		// Read rest of header: ver/cmd, fam/proto, addr-len (4 bytes)
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return nil, nil, fmt.Errorf("failed to read v2 header: %w", err)
		}

		// Continue with parsing
		addr, err := parseProxyProtoV2Header(conn, header)
		return addr, nil, err
	default:
		return nil, nil, fmt.Errorf("unsupported PROXY protocol version: %d", version)
	}
}

// readProxyProtoV2Header is kept for backward compatibility and direct testing.
// It reads and parses a PROXY protocol v2 header from the connection.
// If the command is LOCAL (health check), it returns nil for addr and no error.
// If the command is PROXY, it returns the parsed address information.
// The connection must be fresh (no data read yet).
func readProxyProtoV2Header(conn net.Conn) (*proxyProtoAddr, error) {
	// Set read deadline to prevent hanging on slow/malicious clients
	if err := conn.SetReadDeadline(time.Now().Add(proxyProtoReadTimeout)); err != nil {
		return nil, err
	}
	defer conn.SetReadDeadline(time.Time{})

	// Read fixed header (16 bytes)
	header := make([]byte, proxyProtoV2HeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, errProxyProtoTimeout
		}
		return nil, fmt.Errorf("failed to read PROXY protocol header: %w", err)
	}

	// Validate signature (first 12 bytes)
	if string(header[:12]) != proxyProtoV2Sig {
		return nil, fmt.Errorf("%w: invalid signature", errProxyProtoInvalid)
	}

	// Continue with parsing after signature
	return parseProxyProtoV2Header(conn, header[12:16])
}

// parseProxyProtoV2Header parses v2 protocol after signature has been validated.
// header contains the 4 bytes: ver/cmd, fam/proto, addr-len (2 bytes).
func parseProxyProtoV2Header(conn net.Conn, header []byte) (*proxyProtoAddr, error) {
	// Parse version and command
	verCmd := header[0]
	version := verCmd & proxyProtoV2VerMask
	command := verCmd & proxyProtoCmdMask

	if version != proxyProtoV2Ver {
		return nil, fmt.Errorf("%w: invalid version 0x%02x", errProxyProtoInvalid, version)
	}

	// Parse address family and protocol
	famProto := header[1]
	family := famProto & proxyProtoFamilyMask
	protocol := famProto & proxyProtoProtoMask

	// Parse address length (big-endian uint16)
	addrLen := binary.BigEndian.Uint16(header[2:4])

	// Handle LOCAL command (health check)
	if command == proxyProtoCmdLocal {
		// For LOCAL, we should skip the address data if any
		if addrLen > 0 {
			// Discard the address data
			if _, err := io.CopyN(io.Discard, conn, int64(addrLen)); err != nil {
				return nil, fmt.Errorf("failed to discard LOCAL command address data: %w", err)
			}
		}
		return nil, nil // nil addr indicates LOCAL command
	}

	// Handle PROXY command
	if command != proxyProtoCmdProxy {
		return nil, fmt.Errorf("unknown PROXY protocol command: 0x%02x", command)
	}

	// Validate protocol (we only support STREAM/TCP)
	if protocol != proxyProtoProtoStream {
		return nil, fmt.Errorf("%w: only STREAM protocol supported", errProxyProtoUnsupported)
	}

	// Parse address data based on family
	var addr *proxyProtoAddr
	var err error
	switch family {
	case proxyProtoFamilyInet:
		addr, err = parseIPv4Addr(conn, addrLen)
	case proxyProtoFamilyInet6:
		addr, err = parseIPv6Addr(conn, addrLen)
	case proxyProtoFamilyUnspec:
		// UNSPEC family with PROXY command is valid but rare
		// Just skip the address data
		if addrLen > 0 {
			if _, err := io.CopyN(io.Discard, conn, int64(addrLen)); err != nil {
				return nil, fmt.Errorf("failed to discard UNSPEC address address data: %w", err)
			}
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: unsupported address family 0x%02x", errProxyProtoUnsupported, family)
	}
	return addr, err
}

// parseIPv4Addr parses IPv4 address data from PROXY protocol header
func parseIPv4Addr(conn net.Conn, addrLen uint16) (*proxyProtoAddr, error) {
	// IPv4: 4 (src IP) + 4 (dst IP) + 2 (src port) + 2 (dst port) = 12 bytes minimum
	if addrLen < proxyProtoAddrSizeIPv4 {
		return nil, fmt.Errorf("IPv4 address data too short: %d bytes", addrLen)
	}
	addrData := make([]byte, addrLen)
	if _, err := io.ReadFull(conn, addrData); err != nil {
		return nil, fmt.Errorf("failed to read IPv4 address data: %w", err)
	}
	return &proxyProtoAddr{
		srcIP:   net.IP(addrData[0:4]),
		dstIP:   net.IP(addrData[4:8]),
		srcPort: binary.BigEndian.Uint16(addrData[8:10]),
		dstPort: binary.BigEndian.Uint16(addrData[10:12]),
	}, nil
}

// parseIPv6Addr parses IPv6 address data from PROXY protocol header
func parseIPv6Addr(conn net.Conn, addrLen uint16) (*proxyProtoAddr, error) {
	// IPv6: 16 (src IP) + 16 (dst IP) + 2 (src port) + 2 (dst port) = 36 bytes minimum
	if addrLen < proxyProtoAddrSizeIPv6 {
		return nil, fmt.Errorf("IPv6 address data too short: %d bytes", addrLen)
	}
	addrData := make([]byte, addrLen)
	if _, err := io.ReadFull(conn, addrData); err != nil {
		return nil, fmt.Errorf("failed to read IPv6 address data: %w", err)
	}
	return &proxyProtoAddr{
		srcIP:   net.IP(addrData[0:16]),
		dstIP:   net.IP(addrData[16:32]),
		srcPort: binary.BigEndian.Uint16(addrData[32:34]),
		dstPort: binary.BigEndian.Uint16(addrData[34:36]),
	}, nil
}
