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

import (
	"strings"
	"time"

	"github.com/livekit/protocol/livekit"
)

// sessionAnalytics incrementally aggregates raw per-participant metrics over the
// lifetime of a participant session. It is fed event-driven from the existing
// per-track AnalyticsStat stream (foldTrackStat) and from subscriber-transport BWE
// events (foldTransport). It performs NO derived quality scoring — only raw counters
// and simple min/avg/max aggregates. All access is expected to be serialized by the
// owning StatsWorker; the type itself holds no lock.
type sessionAnalytics struct {
	audioUp  upstreamAgg   // upstream (publisher -> SFU) received audio
	videoUp  upstreamAgg   // upstream received video
	audioFwd downstreamAgg // downstream (SFU -> subscriber) forwarded audio
	videoFwd downstreamAgg // downstream forwarded video

	publishedTracks map[livekit.TrackID]*livekit.TrackInfo // track info for published tracks
}

// SetTrackInfo stores track info for later use during stats aggregation and output.
// Should be called when a track is published or updated.
func (s *sessionAnalytics) SetTrackInfo(trackID livekit.TrackID, info *livekit.TrackInfo) {
	if info == nil {
		return
	}
	if s.publishedTracks == nil {
		s.publishedTracks = make(map[livekit.TrackID]*livekit.TrackInfo)
	}
	s.publishedTracks[trackID] = info
}

// Congestion-control analytics (BWE estimate / target bitrate / overuse-underuse) are
// accumulated in the subscriber transport's StreamAllocator and read at disconnect, so
// they are NOT folded here; they are attached to the proto by the assembly site.

// minMaxAvg accumulates samples to expose min/avg/max. Zero value is ready to use.
type minMaxAvg struct {
	sum  float64
	n    uint64
	minV float64
	maxV float64
}

func (m *minMaxAvg) add(v float64) {
	if m.n == 0 {
		m.minV = v
		m.maxV = v
	} else {
		if v < m.minV {
			m.minV = v
		}
		if v > m.maxV {
			m.maxV = v
		}
	}
	m.sum += v
	m.n++
}

func (m *minMaxAvg) avg() float64 {
	if m.n == 0 {
		return 0
	}
	return m.sum / float64(m.n)
}

// upstreamAgg accumulates received (upstream) transport metrics for one media kind.
// Field names match UpstreamMediaStats for clarity.
type upstreamAgg struct {
	primaryPackets    uint64
	primaryBytes      uint64
	retransmitPackets uint64
	retransmitBytes   uint64
	paddingPackets    uint64
	paddingBytes      uint64
	packetsLost       uint64
	packetsOutOfOrder uint64
	frames            uint32
	nackCount         uint32
	pliCount          uint32
	firCount          uint32

	durationSec float64   // sum of report-window durations (for true avg bitrate)
	bitrate     minMaxAvg // per-report bitrate (bps)
	jitterMs    minMaxAvg // per-report jitter (ms); only non-zero reports
	rttMs       minMaxAvg // per-report rtt (ms); only non-zero reports

	tracks map[livekit.TrackID]struct{}
}

// downstreamAgg accumulates downstream forwarded metrics for one media kind.
// Field names match DownstreamMediaStats for clarity.
type downstreamAgg struct {
	primaryPackets    uint64
	primaryBytes      uint64
	retransmitPackets uint64
	retransmitBytes   uint64
	paddingPackets    uint64
	paddingBytes      uint64
	packetsLost       uint64
	packetsOutOfOrder uint64
	frames            uint32
	nackCount         uint32
	pliCount          uint32
	firCount          uint32

	durationSec float64   // sum of report-window durations (for true avg bitrate)
	bitrate     minMaxAvg // per-report bitrate (bps)
	jitterMs    minMaxAvg // per-report jitter (ms); only non-zero reports
	rttMs       minMaxAvg // per-report rtt (ms); only non-zero reports

	tracks map[livekit.TrackID]struct{}
}

// foldTrackStat folds one per-track AnalyticsStat report into the session aggregate.
// direction selects upstream (received) vs downstream (forwarded); the media kind is
// classified from the stat mime type.
func (s *sessionAnalytics) foldTrackStat(trackID livekit.TrackID, direction livekit.StreamType, stat *livekit.AnalyticsStat) {
	if stat == nil || stat.Mime == "" {
		// cannot attribute a report without a codec/kind; skip.
		return
	}
	isAudio := mimeIsAudio(stat.Mime)

	if direction == livekit.StreamType_DOWNSTREAM {
		fwd := &s.videoFwd
		if isAudio {
			fwd = &s.audioFwd
		}
		fwd.foldDownstream(trackID, stat)
		return
	}

	agg := &s.videoUp
	if isAudio {
		agg = &s.audioUp
	}
	agg.foldUpstream(trackID, stat)
}

func (u *upstreamAgg) foldUpstream(trackID livekit.TrackID, stat *livekit.AnalyticsStat) {
	if u.tracks == nil {
		u.tracks = make(map[livekit.TrackID]struct{})
	}
	u.tracks[trackID] = struct{}{}

	var winBytes uint64
	var winStart, winEnd time.Time
	maxJitterMs := 0.0
	maxRttMs := 0.0

	for _, st := range stat.Streams {
		u.primaryPackets += uint64(st.PrimaryPackets)
		u.primaryBytes += st.PrimaryBytes
		u.retransmitPackets += uint64(st.RetransmitPackets)
		u.retransmitBytes += st.RetransmitBytes
		u.paddingPackets += uint64(st.PaddingPackets)
		u.paddingBytes += st.PaddingBytes
		u.packetsLost += uint64(st.PacketsLost)
		u.packetsOutOfOrder += uint64(st.PacketsOutOfOrder)
		u.frames += st.Frames
		u.nackCount += st.Nacks
		u.pliCount += st.Plis
		u.firCount += st.Firs

		winBytes += st.PrimaryBytes
		winStart, winEnd = unionWindow(winStart, winEnd, st.StartTime.AsTime(), st.EndTime.AsTime())

		if jms := float64(st.Jitter) / 1000.0; jms > maxJitterMs { // proto jitter is microseconds
			maxJitterMs = jms
		}
		if rms := float64(st.Rtt); rms > maxRttMs { // proto rtt is milliseconds
			maxRttMs = rms
		}
	}

	if durSec := winEnd.Sub(winStart).Seconds(); durSec > 0 {
		u.durationSec += durSec
		u.bitrate.add(float64(winBytes) * 8 / durSec)
	}
	if maxJitterMs > 0 {
		u.jitterMs.add(maxJitterMs)
	}
	if maxRttMs > 0 {
		u.rttMs.add(maxRttMs)
	}
}

func (d *downstreamAgg) foldDownstream(trackID livekit.TrackID, stat *livekit.AnalyticsStat) {
	if d.tracks == nil {
		d.tracks = make(map[livekit.TrackID]struct{})
	}
	d.tracks[trackID] = struct{}{}

	var winBytes uint64
	var winStart, winEnd time.Time
	maxJitterMs := 0.0
	maxRttMs := 0.0
	for _, st := range stat.Streams {
		d.primaryPackets += uint64(st.PrimaryPackets)
		d.primaryBytes += st.PrimaryBytes
		d.retransmitPackets += uint64(st.RetransmitPackets)
		d.retransmitBytes += st.RetransmitBytes
		d.paddingPackets += uint64(st.PaddingPackets)
		d.paddingBytes += st.PaddingBytes
		d.packetsLost += uint64(st.PacketsLost)
		d.packetsOutOfOrder += uint64(st.PacketsOutOfOrder)
		d.frames += st.Frames
		d.nackCount += st.Nacks
		d.pliCount += st.Plis
		d.firCount += st.Firs

		winBytes += st.PrimaryBytes
		winStart, winEnd = unionWindow(winStart, winEnd, st.StartTime.AsTime(), st.EndTime.AsTime())

		if jms := float64(st.Jitter) / 1000.0; jms > maxJitterMs { // proto jitter is microseconds
			maxJitterMs = jms
		}
		if rms := float64(st.Rtt); rms > maxRttMs { // proto rtt is milliseconds
			maxRttMs = rms
		}
	}

	if durSec := winEnd.Sub(winStart).Seconds(); durSec > 0 {
		d.durationSec += durSec
		d.bitrate.add(float64(winBytes) * 8 / durSec)
	}
	if maxJitterMs > 0 {
		d.jitterMs.add(maxJitterMs)
	}
	if maxRttMs > 0 {
		d.rttMs.add(maxRttMs)
	}
}

// build renders the accumulated session into a ParticipantAnalytics. id/identity and the
// join/leave timestamps are supplied by the caller (they live on the StatsWorker).
// Congestion, codec metadata and layer-switch counts are filled in by the assembly site
// (they originate outside the telemetry stats stream).
func (s *sessionAnalytics) build(id, identity, roomId string, join, leave time.Time) *ParticipantAnalytics {
	pa := &ParticipantAnalytics{
		ParticipantID:       id,
		ParticipantIdentity: identity,
		RoomId:              roomId,
		Upstream:            s.buildUpstreamStats(),
		Downstream:          s.buildDownstreamStats(),
	}
	if !join.IsZero() {
		pa.JoinTimeUnixMs = join.UnixMilli()
	}
	if !leave.IsZero() {
		pa.LeaveTimeUnixMs = leave.UnixMilli()
	}
	if !join.IsZero() && !leave.IsZero() {
		pa.ParticipationDurationSeconds = leave.Sub(join).Seconds()
	}
	return pa
}

// buildUpstreamStats builds upstream (publisher -> SFU) stats.
func (s *sessionAnalytics) buildUpstreamStats() *ParticipantUpstreamStats {
	audio := s.audioUp.toMediaStats()
	video := s.videoUp.toMediaStats()
	tracks := s.buildTracks()
	if audio == nil && video == nil && tracks == nil {
		return nil
	}
	return &ParticipantUpstreamStats{
		Audio:  audio,
		Video:  video,
		Tracks: tracks,
	}
}

// buildTracks returns the stored published track infos.
func (s *sessionAnalytics) buildTracks() []*livekit.TrackInfo {
	if len(s.publishedTracks) == 0 {
		return nil
	}
	tracks := make([]*livekit.TrackInfo, 0, len(s.publishedTracks))
	for _, ti := range s.publishedTracks {
		tracks = append(tracks, ti)
	}
	return tracks
}

// buildDownstreamStats builds downstream (SFU -> subscriber) stats.
func (s *sessionAnalytics) buildDownstreamStats() *ParticipantDownstreamStats {
	audio := s.audioFwd.toDownstreamStats()
	video := s.videoFwd.toDownstreamStats()
	if audio == nil && video == nil {
		return nil
	}
	return &ParticipantDownstreamStats{
		Audio:                audio,
		Video:                video,
		TracksSubscribed:     uint32(len(s.audioFwd.tracks) + len(s.videoFwd.tracks)),
		AudioTracksForwarded: uint32(len(s.audioFwd.tracks)),
		VideoTracksForwarded: uint32(len(s.videoFwd.tracks)),
	}
}

// toDownstreamStats converts the downstreamAgg to DownstreamMediaStats.
func (d *downstreamAgg) toDownstreamStats() *DownstreamMediaStats {
	if len(d.tracks) == 0 && d.primaryPackets == 0 {
		return nil
	}

	avgBitrate := uint64(0)
	if d.durationSec > 0 {
		avgBitrate = uint64(float64(d.primaryBytes) * 8 / d.durationSec)
	}

	return &DownstreamMediaStats{
		DurationSeconds:   d.durationSec,
		TracksForwarded:   uint32(len(d.tracks)),
		PrimaryPackets:    d.primaryPackets,
		PrimaryBytes:      d.primaryBytes,
		RetransmitPackets: d.retransmitPackets,
		RetransmitBytes:   d.retransmitBytes,
		PaddingPackets:    d.paddingPackets,
		PaddingBytes:      d.paddingBytes,
		PacketsLost:       d.packetsLost,
		PacketsOutOfOrder: d.packetsOutOfOrder,
		Frames:            d.frames,
		BitrateAvgBps:     avgBitrate,
		BitrateMinBps:     uint64(d.bitrate.minV),
		BitrateMaxBps:     uint64(d.bitrate.maxV),
		JitterAvgMs:       d.jitterMs.avg(),
		JitterMaxMs:       d.jitterMs.maxV,
		RttAvgMs:          d.rttMs.avg(),
		RttMaxMs:          d.rttMs.maxV,
		NackCount:         d.nackCount,
		PliCount:          d.pliCount,
		FirCount:          d.firCount,
	}
}

func (u *upstreamAgg) toMediaStats() *UpstreamMediaStats {
	if len(u.tracks) == 0 && u.primaryPackets == 0 {
		return nil
	}
	avgBitrate := uint64(0)
	if u.durationSec > 0 {
		avgBitrate = uint64(float64(u.primaryBytes) * 8 / u.durationSec)
	}

	return &UpstreamMediaStats{
		DurationSeconds:   u.durationSec,
		TracksCount:       uint32(len(u.tracks)),
		PrimaryPackets:    u.primaryPackets,
		PrimaryBytes:      u.primaryBytes,
		RetransmitPackets: u.retransmitPackets,
		RetransmitBytes:   u.retransmitBytes,
		PaddingPackets:    u.paddingPackets,
		PaddingBytes:      u.paddingBytes,
		PacketsLost:       u.packetsLost,
		PacketsOutOfOrder: u.packetsOutOfOrder,
		Frames:            u.frames,
		BitrateAvgBps:     avgBitrate,
		BitrateMinBps:     uint64(u.bitrate.minV),
		BitrateMaxBps:     uint64(u.bitrate.maxV),
		JitterAvgMs:       u.jitterMs.avg(),
		JitterMaxMs:       u.jitterMs.maxV,
		RttAvgMs:          u.rttMs.avg(),
		RttMaxMs:          u.rttMs.maxV,
		NackCount:         u.nackCount,
		PliCount:          u.pliCount,
		FirCount:          u.firCount,
	}
}

// hasData reports whether anything was collected; used to avoid emitting an empty
// analytics payload (e.g. a participant that never connected media).
func (s *sessionAnalytics) hasData() bool {
	return s.audioUp.primaryPackets > 0 || s.videoUp.primaryPackets > 0 ||
		s.audioFwd.primaryPackets > 0 || s.videoFwd.primaryPackets > 0 ||
		len(s.audioFwd.tracks) > 0 || len(s.videoFwd.tracks) > 0
}

func unionWindow(curStart, curEnd, s, e time.Time) (time.Time, time.Time) {
	if curStart.IsZero() || s.Before(curStart) {
		curStart = s
	}
	if e.After(curEnd) {
		curEnd = e
	}
	return curStart, curEnd
}

func mimeIsAudio(m string) bool {
	return strings.HasPrefix(strings.ToLower(m), "audio/")
}
