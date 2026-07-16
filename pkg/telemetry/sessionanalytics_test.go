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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/livekit/protocol/livekit"
)

// TestParticipantAnalytics_JSONShape locks the analytics POST body shape: the
// documented camelCase field names must be present in the JSON.
func TestParticipantAnalytics_JSONShape(t *testing.T) {
	pa := &ParticipantAnalytics{
		ParticipantID: "PA_xxx",
		Upstream: &ParticipantUpstreamStats{
			Audio: &UpstreamMediaStats{PrimaryPackets: 10},
		},
		Downstream: &ParticipantDownstreamStats{
			Congestion: &ParticipantCongestionStats{BweEstimateMaxBps: 2_000_000},
		},
	}
	b, err := json.Marshal(pa)
	require.NoError(t, err)
	js := string(b)
	require.Contains(t, js, "\"participantId\"")
	require.Contains(t, js, "\"upstream\"")
	require.Contains(t, js, "\"primaryPackets\"")
	require.Contains(t, js, "\"downstream\"")
	require.Contains(t, js, "\"congestion\"")
	require.Contains(t, js, "\"bweEstimateMaxBps\"")
}

func TestMinMaxAvg(t *testing.T) {
	var m minMaxAvg
	require.Equal(t, 0.0, m.avg())
	require.Equal(t, 0.0, m.minV)
	require.Equal(t, 0.0, m.maxV)

	m.add(10)
	require.Equal(t, 10.0, m.avg())
	require.Equal(t, 10.0, m.minV)
	require.Equal(t, 10.0, m.maxV)

	m.add(30)
	m.add(20)
	require.Equal(t, 20.0, m.avg())
	require.Equal(t, 10.0, m.minV)
	require.Equal(t, 30.0, m.maxV)
}

// upstreamStat builds an AnalyticsStat for one report window.
func upstreamStat(mimeType string, start, end time.Time, primaryPackets uint32, primaryBytes uint64, lost uint32, jitterUs uint32, rttMs uint32, nacks, plis, firs uint32) *livekit.AnalyticsStat {
	return &livekit.AnalyticsStat{
		Mime: mimeType,
		Streams: []*livekit.AnalyticsStream{{
			StartTime:      timestamppb.New(start),
			EndTime:        timestamppb.New(end),
			PrimaryPackets: primaryPackets,
			PrimaryBytes:   primaryBytes,
			PacketsLost:    lost,
			Jitter:         jitterUs, // microseconds, as produced by connectionstats
			Rtt:            rttMs,    // milliseconds
			Nacks:          nacks,
			Plis:           plis,
			Firs:           firs,
		}},
	}
}

func TestSessionAnalytics_UpstreamAudio(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(1000, 0)

	// two 2-second windows for the same audio track
	s.foldTrackStat("track-a", livekit.StreamType_UPSTREAM,
		upstreamStat("audio/opus", t0, t0.Add(2*time.Second), 100, 16000, 2, 5000, 30, 1, 0, 0))
	s.foldTrackStat("track-a", livekit.StreamType_UPSTREAM,
		upstreamStat("audio/opus", t0.Add(2*time.Second), t0.Add(4*time.Second), 100, 8000, 0, 3000, 50, 0, 0, 0))

	pa := s.build("pid", "identity", "roomId", t0, t0.Add(10*time.Second))
	require.NotNil(t, pa.Upstream)
	require.NotNil(t, pa.Upstream.Audio)
	a := pa.Upstream.Audio

	require.EqualValues(t, 200, a.PrimaryPackets)
	require.EqualValues(t, 24000, a.PrimaryBytes)
	require.EqualValues(t, 2, a.PacketsLost)
	require.EqualValues(t, 1, a.NackCount)
	require.EqualValues(t, 1, a.TracksCount)

	// avg bitrate = 8 * 24000 / 4s = 48000 bps
	require.EqualValues(t, 48000, a.BitrateAvgBps)
	// per-window bitrates: 8*16000/2=64000, 8*8000/2=32000
	require.EqualValues(t, 32000, a.BitrateMinBps)
	require.EqualValues(t, 64000, a.BitrateMaxBps)

	// jitter microseconds -> ms: 5000us=5ms, 3000us=3ms
	require.Equal(t, 5.0, a.JitterMaxMs)
	require.Equal(t, 4.0, a.JitterAvgMs)
	// rtt already ms: max 50, avg 40
	require.Equal(t, 50.0, a.RttMaxMs)
	require.Equal(t, 40.0, a.RttAvgMs)

	// video untouched (nil since no video data)
	require.Nil(t, pa.Upstream.Video)

	// metadata
	require.Equal(t, "pid", pa.ParticipantID)
	require.Equal(t, "identity", pa.ParticipantIdentity)
	require.Equal(t, "roomId", pa.RoomId)
	require.Equal(t, t0.UnixMilli(), pa.JoinTimeUnixMs)
	require.Equal(t, t0.Add(10*time.Second).UnixMilli(), pa.LeaveTimeUnixMs)
	require.Equal(t, 10.0, pa.ParticipationDurationSeconds)
}

func TestSessionAnalytics_ClassifiesByMime(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(2000, 0)

	s.foldTrackStat("a", livekit.StreamType_UPSTREAM,
		upstreamStat("audio/opus", t0, t0.Add(time.Second), 10, 1000, 0, 0, 0, 0, 0, 0))
	s.foldTrackStat("v", livekit.StreamType_UPSTREAM,
		upstreamStat("video/vp8", t0, t0.Add(time.Second), 50, 50000, 5, 0, 0, 0, 3, 1))

	pa := s.build("p", "i", "ri", t0, t0.Add(time.Second))
	require.NotNil(t, pa.Upstream)
	require.EqualValues(t, 10, pa.Upstream.Audio.PrimaryPackets)
	require.EqualValues(t, 50, pa.Upstream.Video.PrimaryPackets)
	require.EqualValues(t, 5, pa.Upstream.Video.PacketsLost)
	require.EqualValues(t, 3, pa.Upstream.Video.PliCount)
	require.EqualValues(t, 1, pa.Upstream.Video.FirCount)
}

func TestSessionAnalytics_DownstreamForwarding(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(3000, 0)

	downstream := func(mimeType, trackID string, start, end time.Time, primary, retransmit, padding uint64) {
		s.foldTrackStat(livekit.TrackID(trackID), livekit.StreamType_DOWNSTREAM, &livekit.AnalyticsStat{
			Mime: mimeType,
			Streams: []*livekit.AnalyticsStream{{
				StartTime:       timestamppb.New(start),
				EndTime:         timestamppb.New(end),
				PrimaryBytes:    primary,
				RetransmitBytes: retransmit,
				PaddingBytes:    padding,
			}},
		})
	}

	downstream("audio/opus", "fa", t0, t0.Add(2*time.Second), 1000, 100, 0)
	downstream("video/vp8", "fv1", t0, t0.Add(2*time.Second), 20000, 500, 200)
	downstream("video/vp8", "fv2", t0, t0.Add(2*time.Second), 30000, 0, 0)

	pa := s.build("p", "i", "ri", t0, t0.Add(2*time.Second))
	d := pa.Downstream
	require.NotNil(t, d)

	// Check summary totals
	require.EqualValues(t, 1, d.AudioTracksForwarded)
	require.EqualValues(t, 2, d.VideoTracksForwarded)
	require.EqualValues(t, 3, d.TracksSubscribed)

	// Check audio downstream stats
	require.NotNil(t, d.Audio)
	require.EqualValues(t, 1000, d.Audio.PrimaryBytes)
	require.EqualValues(t, 100, d.Audio.RetransmitBytes)
	require.Equal(t, 2.0, d.Audio.DurationSeconds)
	require.EqualValues(t, 1, d.Audio.TracksForwarded)

	// Check video downstream stats
	require.NotNil(t, d.Video)
	require.EqualValues(t, 50000, d.Video.PrimaryBytes)  // 20000 + 30000
	require.EqualValues(t, 500, d.Video.RetransmitBytes) // fv1: 500
	require.EqualValues(t, 200, d.Video.PaddingBytes)    // fv1: 200
	// duration sums across concurrently-forwarded tracks (track-seconds): 2 video tracks x 2s
	require.Equal(t, 4.0, d.Video.DurationSeconds)
	require.EqualValues(t, 2, d.Video.TracksForwarded)
}

func TestSessionAnalytics_HasData(t *testing.T) {
	s := &sessionAnalytics{}
	require.False(t, s.hasData())

	t0 := time.Unix(4000, 0)
	s.foldTrackStat("a", livekit.StreamType_UPSTREAM,
		upstreamStat("audio/opus", t0, t0.Add(time.Second), 1, 100, 0, 0, 0, 0, 0, 0))
	require.True(t, s.hasData())
}

func TestSessionAnalytics_SkipsUnclassifiableStat(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(5000, 0)
	// empty mime -> cannot attribute -> skipped
	s.foldTrackStat("x", livekit.StreamType_UPSTREAM,
		upstreamStat("", t0, t0.Add(time.Second), 100, 1000, 0, 0, 0, 0, 0, 0))
	require.False(t, s.hasData())
}

func TestMimeIsAudio(t *testing.T) {
	require.True(t, mimeIsAudio("audio/opus"))
	require.True(t, mimeIsAudio("AUDIO/RED"))
	require.False(t, mimeIsAudio("video/vp8"))
	require.False(t, mimeIsAudio(""))
}

func TestSessionAnalytics_SetTrackInfo(t *testing.T) {
	s := &sessionAnalytics{}

	// nil info should be ignored
	s.SetTrackInfo("track-1", nil)
	require.Nil(t, s.publishedTracks)

	// valid track info should be stored
	ti := &livekit.TrackInfo{
		Sid:      "TR_abc",
		Type:     livekit.TrackType_VIDEO,
		Name:     "camera",
		MimeType: "video/vp8",
		Source:   livekit.TrackSource_CAMERA,
	}
	s.SetTrackInfo("track-1", ti)
	require.NotNil(t, s.publishedTracks)
	require.Equal(t, ti, s.publishedTracks["track-1"])

	// update existing track
	ti2 := &livekit.TrackInfo{
		Sid:      "TR_abc",
		Type:     livekit.TrackType_VIDEO,
		Name:     "camera",
		MimeType: "video/vp8",
		Width:    1280,
		Height:   720,
	}
	s.SetTrackInfo("track-1", ti2)
	require.Equal(t, ti2, s.publishedTracks["track-1"])
}

func TestSessionAnalytics_TracksInUpstream(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(6000, 0)

	// Add track info
	ti := &livekit.TrackInfo{
		Sid:      "TR_video",
		Type:     livekit.TrackType_VIDEO,
		Name:     "camera",
		MimeType: "video/vp8",
	}
	s.SetTrackInfo("track-v", ti)

	// Add some upstream stats
	s.foldTrackStat("track-v", livekit.StreamType_UPSTREAM,
		upstreamStat("video/vp8", t0, t0.Add(time.Second), 100, 50000, 0, 0, 0, 0, 0, 0))

	pa := s.build("p", "i", "ri", t0, t0.Add(time.Second))

	// Tracks should be in Upstream
	require.NotNil(t, pa.Upstream)
	require.NotNil(t, pa.Upstream.Tracks)
	require.Len(t, pa.Upstream.Tracks, 1)
	require.Equal(t, "TR_video", pa.Upstream.Tracks[0].Sid)
}

func TestSessionAnalytics_DownstreamStructure(t *testing.T) {
	// Test that downstream stats have the correct structure
	pa := &ParticipantAnalytics{
		Downstream: &ParticipantDownstreamStats{
			Audio: &DownstreamMediaStats{
				PrimaryPackets: 100,
				PrimaryBytes:   10000,
			},
			Video: &DownstreamMediaStats{
				PrimaryPackets: 500,
				PrimaryBytes:   500000,
			},
			TracksSubscribed:          3,
			AudioTracksForwarded:      1,
			VideoTracksForwarded:      2,
			SimulcastLayerSwitchCount: 5,
			SvcLayerSwitchCount:       2,
			Congestion: &ParticipantCongestionStats{
				BweEstimateAvgBps:  1_500_000,
				BweEstimateMinBps:  500_000,
				BweEstimateMaxBps:  3_000_000,
				OveruseEventCount:  2,
				UnderuseEventCount: 1,
			},
		},
	}

	b, err := json.Marshal(pa)
	require.NoError(t, err)
	js := string(b)

	// Verify all downstream fields are present in JSON
	require.Contains(t, js, "\"downstream\"")
	require.Contains(t, js, "\"audio\"")
	require.Contains(t, js, "\"video\"")
	require.Contains(t, js, "\"tracksSubscribed\"")
	require.Contains(t, js, "\"simulcastLayerSwitchCount\"")
	require.Contains(t, js, "\"svcLayerSwitchCount\"")
	require.Contains(t, js, "\"congestion\"")
	require.Contains(t, js, "\"bweEstimateAvgBps\"")
	require.Contains(t, js, "\"overuseEventCount\"")
}

func TestSessionAnalytics_UpstreamAllFields(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(7000, 0)

	// Create stat with all fields populated
	stat := &livekit.AnalyticsStat{
		Mime: "video/vp8",
		Streams: []*livekit.AnalyticsStream{{
			StartTime:         timestamppb.New(t0),
			EndTime:           timestamppb.New(t0.Add(2 * time.Second)),
			PrimaryPackets:    100,
			PrimaryBytes:      50000,
			RetransmitPackets: 5,
			RetransmitBytes:   2500,
			PaddingPackets:    10,
			PaddingBytes:      1000,
			PacketsLost:       3,
			PacketsOutOfOrder: 2,
			Frames:            60,
			Jitter:            5000, // 5ms in microseconds
			Rtt:               25,   // 25ms
			Nacks:             2,
			Plis:              1,
			Firs:              0,
		}},
	}

	s.foldTrackStat("track-v", livekit.StreamType_UPSTREAM, stat)

	pa := s.build("p", "i", "ri", t0, t0.Add(2*time.Second))
	require.NotNil(t, pa.Upstream)
	require.NotNil(t, pa.Upstream.Video)

	v := pa.Upstream.Video
	require.EqualValues(t, 100, v.PrimaryPackets)
	require.EqualValues(t, 50000, v.PrimaryBytes)
	require.EqualValues(t, 5, v.RetransmitPackets)
	require.EqualValues(t, 2500, v.RetransmitBytes)
	require.EqualValues(t, 10, v.PaddingPackets)
	require.EqualValues(t, 1000, v.PaddingBytes)
	require.EqualValues(t, 3, v.PacketsLost)
	require.EqualValues(t, 2, v.PacketsOutOfOrder)
	require.EqualValues(t, 60, v.Frames)
	require.EqualValues(t, 2, v.NackCount)
	require.EqualValues(t, 1, v.PliCount)
	require.EqualValues(t, 0, v.FirCount)

	// bitrate = 50000 * 8 / 2 = 200000 bps
	require.EqualValues(t, 200000, v.BitrateAvgBps)
	require.Equal(t, 5.0, v.JitterMaxMs)
	require.Equal(t, 25.0, v.RttMaxMs)
}

func TestSessionAnalytics_DownstreamAllFields(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(8000, 0)

	// Create downstream stat with all fields populated
	stat := &livekit.AnalyticsStat{
		Mime: "video/vp8",
		Streams: []*livekit.AnalyticsStream{{
			StartTime:         timestamppb.New(t0),
			EndTime:           timestamppb.New(t0.Add(2 * time.Second)),
			PrimaryPackets:    200,
			PrimaryBytes:      100000,
			RetransmitPackets: 10,
			RetransmitBytes:   5000,
			PaddingPackets:    20,
			PaddingBytes:      2000,
			PacketsLost:       5,
			PacketsOutOfOrder: 3,
			Frames:            120,
			Jitter:            3000, // 3ms in microseconds
			Rtt:               15,   // 15ms
			Nacks:             4,
			Plis:              2,
			Firs:              1,
		}},
	}

	s.foldTrackStat("track-v", livekit.StreamType_DOWNSTREAM, stat)

	pa := s.build("p", "i", "ri", t0, t0.Add(2*time.Second))
	require.NotNil(t, pa.Downstream)
	require.NotNil(t, pa.Downstream.Video)

	v := pa.Downstream.Video
	require.EqualValues(t, 200, v.PrimaryPackets)
	require.EqualValues(t, 100000, v.PrimaryBytes)
	require.EqualValues(t, 10, v.RetransmitPackets)
	require.EqualValues(t, 5000, v.RetransmitBytes)
	require.EqualValues(t, 20, v.PaddingPackets)
	require.EqualValues(t, 2000, v.PaddingBytes)
	require.EqualValues(t, 5, v.PacketsLost)
	require.EqualValues(t, 3, v.PacketsOutOfOrder)
	require.EqualValues(t, 120, v.Frames)
	require.EqualValues(t, 4, v.NackCount)
	require.EqualValues(t, 2, v.PliCount)
	require.EqualValues(t, 1, v.FirCount)
	require.EqualValues(t, 1, v.TracksForwarded)
	require.Equal(t, 2.0, v.DurationSeconds)

	// bitrate = 100000 * 8 / 2 = 400000 bps
	require.EqualValues(t, 400000, v.BitrateAvgBps)
	require.Equal(t, 3.0, v.JitterMaxMs)
	require.Equal(t, 15.0, v.RttMaxMs)
}

func TestSessionAnalytics_MultipleStreamsInStat(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(9000, 0)

	// Simulcast scenario: multiple streams in one stat (different quality layers)
	stat := &livekit.AnalyticsStat{
		Mime: "video/vp8",
		Streams: []*livekit.AnalyticsStream{
			{
				StartTime:      timestamppb.New(t0),
				EndTime:        timestamppb.New(t0.Add(2 * time.Second)),
				PrimaryPackets: 50,
				PrimaryBytes:   10000,
				Jitter:         2000,
				Rtt:            20,
			},
			{
				StartTime:      timestamppb.New(t0),
				EndTime:        timestamppb.New(t0.Add(2 * time.Second)),
				PrimaryPackets: 100,
				PrimaryBytes:   30000,
				Jitter:         3000,
				Rtt:            25,
			},
			{
				StartTime:      timestamppb.New(t0),
				EndTime:        timestamppb.New(t0.Add(2 * time.Second)),
				PrimaryPackets: 150,
				PrimaryBytes:   60000,
				Jitter:         4000,
				Rtt:            30,
			},
		},
	}

	s.foldTrackStat("track-v", livekit.StreamType_UPSTREAM, stat)

	pa := s.build("p", "i", "ri", t0, t0.Add(2*time.Second))
	require.NotNil(t, pa.Upstream)
	require.NotNil(t, pa.Upstream.Video)

	v := pa.Upstream.Video
	// All streams should be summed
	require.EqualValues(t, 300, v.PrimaryPackets)  // 50+100+150
	require.EqualValues(t, 100000, v.PrimaryBytes) // 10000+30000+60000

	// Max jitter/rtt across all streams
	require.Equal(t, 4.0, v.JitterMaxMs) // max of 2,3,4 ms
	require.Equal(t, 30.0, v.RttMaxMs)   // max of 20,25,30 ms

	// Bitrate: total bytes / union window time
	// Union window is T0 to T0+2s for all streams = 2 seconds
	// 100000 * 8 / 2 = 400000 bps
	require.EqualValues(t, 400000, v.BitrateAvgBps)
}

func TestSessionAnalytics_EmptyUpstreamWhenNoData(t *testing.T) {
	s := &sessionAnalytics{}
	t0 := time.Unix(10000, 0)

	// Only add track info, no stats
	s.SetTrackInfo("track-v", &livekit.TrackInfo{Sid: "TR_v"})

	pa := s.build("p", "i", "ri", t0, t0.Add(time.Second))

	// Upstream should exist (because of tracks) but audio/video should be nil
	require.NotNil(t, pa.Upstream)
	require.NotNil(t, pa.Upstream.Tracks)
	require.Nil(t, pa.Upstream.Audio)
	require.Nil(t, pa.Upstream.Video)
}
