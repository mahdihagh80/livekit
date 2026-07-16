package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStatsWorker(t *testing.T) {
	t.Run("reference counted close works", func(t *testing.T) {
		var g0, g1 ReferenceGuard
		w := newStatsWorker(t.Context(), nil, "", "", "", "", &g0)
		require.False(t, w.Closed(&g1))
		require.False(t, w.Close(&g0))
		require.False(t, w.Closed(&g1))
		require.True(t, w.Close(&g1))
		require.True(t, w.Closed(&g1))
	})

	t.Run("logging a nil worker does not panic", func(t *testing.T) {
		var w *StatsWorker
		require.NoError(t, w.MarshalLogObject(zapcore.NewMapObjectEncoder()))
	})
}

func TestGetOrCreateWorkerReleasedGuard(t *testing.T) {
	// ParticipantActive overtaken by the participant's close arrives with a guard that
	// ParticipantLeft already released. It must not replace the closed worker with one
	// nothing can release.
	ts := &telemetryService{workers: make(map[statsWorkerKey]*StatsWorker)}
	roomID, pID := livekit.RoomID("room"), livekit.ParticipantID("participant")

	var g ReferenceGuard
	w, found := ts.getOrCreateWorker(context.Background(), roomID, "", pID, "", &g)
	require.False(t, found)
	require.True(t, w.Close(&g))

	t.Run("closed worker still in the map", func(t *testing.T) {
		late, found := ts.getOrCreateWorker(context.Background(), roomID, "", pID, "", &g)
		require.True(t, found)
		require.Same(t, w, late)
		require.Same(t, w, ts.workers[statsWorkerKey{roomID, pID}])
	})

	t.Run("closed worker already reaped", func(t *testing.T) {
		delete(ts.workers, statsWorkerKey{roomID, pID})

		late, found := ts.getOrCreateWorker(context.Background(), roomID, "", pID, "", &g)
		require.True(t, found)
		require.Nil(t, late)
		require.Empty(t, ts.workers)
		late.SetConnected()
	})
}

func newTestStatsWorker() *StatsWorker {
	return newStatsWorker(
		context.Background(),
		&NullTelemetryService{},
		"room-id",
		"room-name",
		"participant-id",
		"identity",
		nil,
	)
}

func TestStatsWorker_BuildAnalytics(t *testing.T) {
	w := newTestStatsWorker()
	t0 := time.Unix(1000, 0)

	w.OnTrackStat("track-a", livekit.StreamType_UPSTREAM, &livekit.AnalyticsStat{
		Mime: "audio/opus",
		Streams: []*livekit.AnalyticsStream{{
			StartTime:      timestamppb.New(t0),
			EndTime:        timestamppb.New(t0.Add(time.Second)),
			PrimaryPackets: 50,
			PrimaryBytes:   8000,
			PacketsLost:    1,
		}},
	})
	w.OnTrackStat("track-v", livekit.StreamType_DOWNSTREAM, &livekit.AnalyticsStat{
		Mime: "video/vp8",
		Streams: []*livekit.AnalyticsStream{{
			StartTime:    timestamppb.New(t0),
			EndTime:      timestamppb.New(t0.Add(time.Second)),
			PrimaryBytes: 40000,
		}},
	})

	pa := w.BuildAnalytics(t0, t0.Add(5*time.Second))
	require.NotNil(t, pa)
	require.Equal(t, "participant-id", pa.ParticipantID)
	require.Equal(t, "identity", pa.ParticipantIdentity)
	require.NotNil(t, pa.Upstream)
	require.NotNil(t, pa.Upstream.Audio)
	require.EqualValues(t, 50, pa.Upstream.Audio.PrimaryPackets)
	require.EqualValues(t, 1, pa.Upstream.Audio.PacketsLost)
	require.NotNil(t, pa.Downstream)
	require.NotNil(t, pa.Downstream.Video)
	require.EqualValues(t, 40000, pa.Downstream.Video.PrimaryBytes)
	require.EqualValues(t, 1, pa.Downstream.VideoTracksForwarded)
	require.Equal(t, 5.0, pa.ParticipationDurationSeconds)
}

func TestStatsWorker_BuildAnalytics_NoData(t *testing.T) {
	w := newTestStatsWorker()
	require.Nil(t, w.BuildAnalytics(time.Now(), time.Now()))
}
