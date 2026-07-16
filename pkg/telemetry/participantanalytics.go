// Copyright 2024 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import "github.com/livekit/protocol/livekit"

// Per-participant call analytics, defined in livekit-server (no protocol/proto
// dependency). Assembled once at participant disconnect and delivered out-of-band
// via a dedicated signed POST (see ParticipantAnalyticsSink), independent of the
// participant_left webhook. Contains only raw counters/aggregates collected from
// SFU internals; NO derived quality scores. JSON field names mirror the previous
// protojson camelCase shape so downstream parsers stay familiar.
// To enable analytics, set participant_analytics_url in the config.
type ParticipantAnalytics struct {
	ParticipantID                string  `json:"participantId,omitempty"`
	ParticipantIdentity          string  `json:"participantIdentity,omitempty"`
	RoomId                       string  `json:"roomId,omitempty"`
	NodeIP                       string  `json:"nodeIp,omitempty"`
	ParticipantIP                string  `json:"participantIp,omitempty"`
	ParticipantASN               string  `json:"participantAsn,omitempty"`
	JoinTimeUnixMs               int64   `json:"joinTimeUnixMs,omitempty"`
	LeaveTimeUnixMs              int64   `json:"leaveTimeUnixMs,omitempty"`
	ParticipationDurationSeconds float64 `json:"participationDurationSeconds,omitempty"`

	// upstream (publisher -> SFU) received media
	Upstream *ParticipantUpstreamStats `json:"upstream,omitempty"`

	// downstream (SFU -> subscriber) forwarded media
	Downstream *ParticipantDownstreamStats `json:"downstream,omitempty"`

	// client/device info reported by the participant's SDK at connect time
	Client *ParticipantClientInfo `json:"client,omitempty"`
}

// Client/device metadata as reported by the participant's SDK on connect (see
// livekit.ClientInfo), plus the geo-resolved country. Purely descriptive, no
// derived metrics.
type ParticipantClientInfo struct {
	Sdk             string `json:"sdk,omitempty"`
	SdkVersion      string `json:"sdkVersion,omitempty"`
	ProtocolVersion int32  `json:"protocolVersion,omitempty"`
	Os              string `json:"os,omitempty"`
	OsVersion       string `json:"osVersion,omitempty"`
	DeviceModel     string `json:"deviceModel,omitempty"`
	Browser         string `json:"browser,omitempty"`
	BrowserVersion  string `json:"browserVersion,omitempty"`
	// wifi, wired, cellular, vpn, empty if not known
	Network string `json:"network,omitempty"`
	// comma separated list of additional LiveKit SDKs in use, with versions
	OtherSdks string `json:"otherSdks,omitempty"`
	Country   string `json:"country,omitempty"`
}

// UpstreamMediaStats contains raw transport metrics for upstream (publisher -> SFU) media.
// Counters are summed over the session; bitrate/jitter/rtt aggregates are computed over
// the periodic (~5s) stats reports. JitterAvgMs / RttAvgMs are the mean of per-report
// values (the underlying stats expose per-report max, not a true per-packet mean).
type UpstreamMediaStats struct {
	DurationSeconds float64 `json:"durationSeconds,omitempty"`
	TracksCount     uint32  `json:"tracksCount,omitempty"`

	// Packet and byte counters (same names as livekit.AnalyticsStream)
	PrimaryPackets    uint64 `json:"primaryPackets,omitempty"`
	PrimaryBytes      uint64 `json:"primaryBytes,omitempty"`
	RetransmitPackets uint64 `json:"retransmitPackets,omitempty"`
	RetransmitBytes   uint64 `json:"retransmitBytes,omitempty"`
	PaddingPackets    uint64 `json:"paddingPackets,omitempty"`
	PaddingBytes      uint64 `json:"paddingBytes,omitempty"`
	PacketsLost       uint64 `json:"packetsLost,omitempty"`
	PacketsOutOfOrder uint64 `json:"packetsOutOfOrder,omitempty"`
	Frames            uint32 `json:"frames,omitempty"`

	// Quality indicators
	BitrateAvgBps uint64  `json:"bitrateAvgBps,omitempty"`
	BitrateMinBps uint64  `json:"bitrateMinBps,omitempty"`
	BitrateMaxBps uint64  `json:"bitrateMaxBps,omitempty"`
	JitterAvgMs   float64 `json:"jitterAvgMs,omitempty"`
	JitterMaxMs   float64 `json:"jitterMaxMs,omitempty"`
	RttAvgMs      float64 `json:"rttAvgMs,omitempty"`
	RttMaxMs      float64 `json:"rttMaxMs,omitempty"`

	// Recovery requests
	NackCount uint32 `json:"nackCount,omitempty"`
	PliCount  uint32 `json:"pliCount,omitempty"`
	FirCount  uint32 `json:"firCount,omitempty"`
}

// ParticipantUpstreamStats contains media received from the participant (publisher -> SFU).
type ParticipantUpstreamStats struct {
	Audio *UpstreamMediaStats `json:"audio,omitempty"`
	Video *UpstreamMediaStats `json:"video,omitempty"`

	// Published tracks info (codec, layers, source, etc.)
	Tracks []*livekit.TrackInfo `json:"tracks,omitempty"`
}

// ParticipantDownstreamStats contains media forwarded to the participant (SFU -> subscriber).
type ParticipantDownstreamStats struct {
	Audio *DownstreamMediaStats `json:"audio,omitempty"`
	Video *DownstreamMediaStats `json:"video,omitempty"`

	// Summary totals
	TracksSubscribed     uint32 `json:"tracksSubscribed,omitempty"`
	AudioTracksForwarded uint32 `json:"audioTracksForwarded,omitempty"`
	VideoTracksForwarded uint32 `json:"videoTracksForwarded,omitempty"`

	// Layer switch counts (subscriber-side, when SFU changes forwarded layer)
	SimulcastLayerSwitchCount uint32 `json:"simulcastLayerSwitchCount,omitempty"`
	SvcLayerSwitchCount       uint32 `json:"svcLayerSwitchCount,omitempty"`

	// Subscriber transport congestion control (send-side BWE)
	Congestion *ParticipantCongestionStats `json:"congestion,omitempty"`
}

// DownstreamMediaStats contains per-media-kind stats for what was forwarded to a subscriber.
// Useful for troubleshooting specific issues like "can't hear audio" or "video freezing".
type DownstreamMediaStats struct {
	DurationSeconds float64 `json:"durationSeconds,omitempty"`
	TracksForwarded uint32  `json:"tracksForwarded,omitempty"`

	// Packet and byte counters (same names as livekit.AnalyticsStream)
	PrimaryPackets    uint64 `json:"primaryPackets,omitempty"`
	PrimaryBytes      uint64 `json:"primaryBytes,omitempty"`
	RetransmitPackets uint64 `json:"retransmitPackets,omitempty"`
	RetransmitBytes   uint64 `json:"retransmitBytes,omitempty"`
	PaddingPackets    uint64 `json:"paddingPackets,omitempty"`
	PaddingBytes      uint64 `json:"paddingBytes,omitempty"`
	PacketsLost       uint64 `json:"packetsLost,omitempty"`
	PacketsOutOfOrder uint64 `json:"packetsOutOfOrder,omitempty"`
	Frames            uint32 `json:"frames,omitempty"`

	// Quality indicators
	BitrateAvgBps uint64  `json:"bitrateAvgBps,omitempty"`
	BitrateMinBps uint64  `json:"bitrateMinBps,omitempty"`
	BitrateMaxBps uint64  `json:"bitrateMaxBps,omitempty"`
	JitterAvgMs   float64 `json:"jitterAvgMs,omitempty"`
	JitterMaxMs   float64 `json:"jitterMaxMs,omitempty"`
	RttAvgMs      float64 `json:"rttAvgMs,omitempty"`
	RttMaxMs      float64 `json:"rttMaxMs,omitempty"`

	// Recovery requests from subscriber
	NackCount uint32 `json:"nackCount,omitempty"`
	PliCount  uint32 `json:"pliCount,omitempty"`
	FirCount  uint32 `json:"firCount,omitempty"`
}

// Send-side bandwidth estimation on the subscriber transport. For an SFU using
// send-side BWE, the available outgoing bitrate and the subscriber bandwidth
// estimate are the SAME value as the BWE estimate (kept as a single set).
type ParticipantCongestionStats struct {
	BweEstimateAvgBps   uint64 `json:"bweEstimateAvgBps,omitempty"`
	BweEstimateMinBps   uint64 `json:"bweEstimateMinBps,omitempty"`
	BweEstimateMaxBps   uint64 `json:"bweEstimateMaxBps,omitempty"`
	TargetBitrateAvgBps uint64 `json:"targetBitrateAvgBps,omitempty"`
	TargetBitrateMinBps uint64 `json:"targetBitrateMinBps,omitempty"`
	TargetBitrateMaxBps uint64 `json:"targetBitrateMaxBps,omitempty"`
	// congestion-state transitions toward a more congested state
	OveruseEventCount uint32 `json:"overuseEventCount,omitempty"`
	// congestion-state transitions toward a less congested state
	UnderuseEventCount uint32 `json:"underuseEventCount,omitempty"`
}
