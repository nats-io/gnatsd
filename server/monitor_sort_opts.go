// Copyright 2013-2025 The NATS Authors
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
	"time"
)

// ConnInfos represents a connection info list. We use pointers since it will be sorted.
type ConnInfos []*ConnInfo

// For sorting
// Len returns length for sorting.
func (cl ConnInfos) Len() int { return len(cl) }

// Swap will sawap the elements.
func (cl ConnInfos) Swap(i, j int) { cl[i], cl[j] = cl[j], cl[i] }

// SortOpt is a helper type to sort clients
type SortOpt string

// Possible sort options
const (
	ByCid      SortOpt = "cid"        // By connection ID
	ByStart    SortOpt = "start"      // By connection start time, same as CID
	BySubs     SortOpt = "subs"       // By number of subscriptions
	ByPending  SortOpt = "pending"    // By amount of data in bytes waiting to be sent to client
	ByOutMsgs  SortOpt = "msgs_to"    // By number of messages sent
	ByInMsgs   SortOpt = "msgs_from"  // By number of messages received
	ByOutBytes SortOpt = "bytes_to"   // By amount of bytes sent
	ByInBytes  SortOpt = "bytes_from" // By amount of bytes received
	ByLast     SortOpt = "last"       // By the last activity
	ByIdle     SortOpt = "idle"       // By the amount of inactivity
	ByUptime   SortOpt = "uptime"     // By the amount of time connections exist
	ByStop     SortOpt = "stop"       // By the stop time for a closed connection
	ByReason   SortOpt = "reason"     // By the reason for a closed connection
	ByRTT      SortOpt = "rtt"        // By the round trip time
)

// Individual sort options provide the Less for sort.Interface. Len and Swap are on cList.
// Ties are broken by cid, so the order is the same on every call, e.g. when paging.
// CID
type SortByCid struct{ ConnInfos }

func (l SortByCid) Less(i, j int) bool { return l.ConnInfos[i].Cid < l.ConnInfos[j].Cid }

// Number of Subscriptions
type SortBySubs struct{ ConnInfos }

func (l SortBySubs) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.NumSubs != cj.NumSubs {
		return ci.NumSubs < cj.NumSubs
	}
	return SortByCid(l).Less(i, j)
}

// Pending Bytes
type SortByPending struct{ ConnInfos }

func (l SortByPending) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.Pending != cj.Pending {
		return ci.Pending < cj.Pending
	}
	return SortByCid(l).Less(i, j)
}

// Outbound Msgs
type SortByOutMsgs struct{ ConnInfos }

func (l SortByOutMsgs) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.OutMsgs != cj.OutMsgs {
		return ci.OutMsgs < cj.OutMsgs
	}
	return SortByCid(l).Less(i, j)
}

// Inbound Msgs
type SortByInMsgs struct{ ConnInfos }

func (l SortByInMsgs) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.InMsgs != cj.InMsgs {
		return ci.InMsgs < cj.InMsgs
	}
	return SortByCid(l).Less(i, j)
}

// Outbound Bytes
type SortByOutBytes struct{ ConnInfos }

func (l SortByOutBytes) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.OutBytes != cj.OutBytes {
		return ci.OutBytes < cj.OutBytes
	}
	return SortByCid(l).Less(i, j)
}

// Inbound Bytes
type SortByInBytes struct{ ConnInfos }

func (l SortByInBytes) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.InBytes != cj.InBytes {
		return ci.InBytes < cj.InBytes
	}
	return SortByCid(l).Less(i, j)
}

// Last Activity
type SortByLast struct{ ConnInfos }

func (l SortByLast) Less(i, j int) bool {
	lasti, lastj := l.ConnInfos[i].LastActivity.UnixNano(), l.ConnInfos[j].LastActivity.UnixNano()
	if lasti != lastj {
		return lasti < lastj
	}
	return SortByCid(l).Less(i, j)
}

// Idle time
type SortByIdle struct {
	ConnInfos
	now time.Time
}

func (l SortByIdle) Less(i, j int) bool {
	idlei, idlej := l.now.Sub(l.ConnInfos[i].LastActivity), l.now.Sub(l.ConnInfos[j].LastActivity)
	if idlei != idlej {
		return idlei < idlej
	}
	return SortByCid{l.ConnInfos}.Less(i, j)
}

// Uptime
type SortByUptime struct {
	ConnInfos
	now time.Time
}

func (l SortByUptime) Less(i, j int) bool {
	ci := l.ConnInfos[i]
	cj := l.ConnInfos[j]
	var upi, upj time.Duration
	if ci.Stop == nil || ci.Stop.IsZero() {
		upi = l.now.Sub(ci.Start)
	} else {
		upi = ci.Stop.Sub(ci.Start)
	}
	if cj.Stop == nil || cj.Stop.IsZero() {
		upj = l.now.Sub(cj.Start)
	} else {
		upj = cj.Stop.Sub(cj.Start)
	}
	if upi != upj {
		return upi < upj
	}
	return SortByCid{l.ConnInfos}.Less(i, j)
}

// Stop
type SortByStop struct{ ConnInfos }

func (l SortByStop) Less(i, j int) bool {
	ciStop := l.ConnInfos[i].Stop
	cjStop := l.ConnInfos[j].Stop
	if !ciStop.Equal(*cjStop) {
		return ciStop.Before(*cjStop)
	}
	return SortByCid(l).Less(i, j)
}

// Reason
type SortByReason struct{ ConnInfos }

func (l SortByReason) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.Reason != cj.Reason {
		return ci.Reason < cj.Reason
	}
	return SortByCid(l).Less(i, j)
}

// RTT - Default is descending
type SortByRTT struct{ ConnInfos }

func (l SortByRTT) Less(i, j int) bool {
	ci, cj := l.ConnInfos[i], l.ConnInfos[j]
	if ci.rtt != cj.rtt {
		return ci.rtt < cj.rtt
	}
	return SortByCid(l).Less(i, j)
}

// IsValid determines if a sort option is valid
func (s SortOpt) IsValid() bool {
	switch s {
	case _EMPTY_, ByCid, ByStart, BySubs, ByPending, ByOutMsgs, ByInMsgs, ByOutBytes, ByInBytes, ByLast, ByIdle, ByUptime, ByStop, ByReason, ByRTT:
		return true
	default:
		return false
	}
}
