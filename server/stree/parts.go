// Copyright 2023-2026 The NATS Authors
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

package stree

import (
	"strings"
)

// Wildcard parts share these backing arrays and are recognized by identity.
// A literal part trimmed down to "*" or ">" must not be mistaken for a wildcard.
var (
	pwcPart = []byte{pwc}
	fwcPart = []byte{fwc}
)

// isPWCPart returns true if the part is a pwc placeholder from genParts.
func isPWCPart(part []byte) bool {
	return len(part) == 1 && &part[0] == &pwcPart[0]
}

// isFWCPart returns true if the part is a fwc placeholder from genParts.
func isFWCPart(part []byte) bool {
	return len(part) == 1 && &part[0] == &fwcPart[0]
}

// genParts will break a filter subject up into parts.
// We need to break this up into chunks based on wildcards, either pwc '*' or fwc '>'.
// We do not care about other tokens per se, just parts that are separated by wildcards with an optional end fwc.
func genParts(filter []byte, parts [][]byte) [][]byte {
	var start int
	for i, e := 0, len(filter)-1; i < len(filter); i++ {
		if filter[i] == tsep {
			// See if next token is pwc. Either internal or end pwc.
			if i < e && filter[i+1] == pwc && (i+2 <= e && filter[i+2] == tsep || i+1 == e) {
				if i > start {
					parts = append(parts, filter[start:i+1])
				}
				parts = append(parts, pwcPart)
				i++ // Skip pwc
				if i+2 <= e {
					i++ // Skip next tsep from next part too.
				}
				start = i + 1
			} else if i < e && filter[i+1] == fwc && i+1 == e {
				if i > start {
					parts = append(parts, filter[start:i+1])
				}
				parts = append(parts, fwcPart)
				i++ // Skip fwc
				start = i + 1
			}
		} else if filter[i] == pwc || filter[i] == fwc {
			// Wildcard must be at the start or preceded by tsep.
			if prev := i - 1; prev >= 0 && filter[prev] != tsep {
				continue
			}
			// Wildcard must be at the end or followed by tsep.
			if next := i + 1; next == e || next < e && filter[next] != tsep {
				continue
			}
			// Full wildcard must be terminal.
			if filter[i] == fwc && i < e {
				break
			}
			// We start with a pwc or fwc.
			if filter[i] == pwc {
				parts = append(parts, pwcPart)
			} else {
				parts = append(parts, fwcPart)
			}
			if i+1 <= e {
				i++ // Skip next tsep from next part too.
			}
			start = i + 1
		}
	}
	if start < len(filter) {
		// Check to see if we need to eat a leading tsep.
		if filter[start] == tsep {
			start++
		}
		parts = append(parts, filter[start:])
	}
	return parts
}

// Match our parts against a stored fragment, which could be a prefix for nodes
// or a suffix for leaves.
func matchParts(parts [][]byte, frag string) ([][]byte, bool) {
	lf := len(frag)
	if lf == 0 {
		return parts, true
	}

	var si int
	lpi := len(parts) - 1

	for i, part := range parts {
		if si >= lf {
			return parts[i:], true
		}
		lp := len(part)
		// Check for pwc or fwc place holders.
		if lp == 1 {
			if isPWCPart(part) {
				index := strings.IndexByte(frag[si:], tsep)
				// We are trying to match pwc and did not find our tsep.
				// Will need to move to next node from caller.
				if index < 0 {
					if i == lpi {
						return nil, true
					}
					return parts[i:], true
				}
				si += index + 1
				continue
			} else if isFWCPart(part) {
				// If we are here we should be good.
				return nil, true
			}
		}
		end := min(si+lp, lf)
		// If part is bigger then the remaining fragment, adjust to a portion on the part.
		if si+lp > end {
			// Frag is smaller then part itself.
			part = part[:end-si]
		}
		if string(part) != frag[si:end] {
			return parts, false
		}
		// If we still have a portion of the fragment left, update and continue.
		if end < lf {
			si = end
			continue
		}
		// If we matched a partial, do not move past current part
		// but update the part to what was consumed. This allows upper layers to continue.
		if end < si+lp {
			if end >= lf {
				// Create a copy before modifying. Reuse slice capacity available at the
				// end of the parts slice, since this saves us additional allocations.
				lp := len(parts)
				parts = append(parts[lp:], parts[:lp]...)
				parts[i] = parts[i][lf-si:]
			} else {
				i++
			}
			return parts[i:], true
		}
		if i == lpi {
			return nil, true
		}
		// If we are here we are not the last part which means we have a wildcard
		// gap, so we need to match anything up to next tsep.
		si += len(part)
	}
	return parts, false
}
