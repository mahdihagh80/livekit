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

package streamallocator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/sfu/bwe"
)

func TestMmAccum(t *testing.T) {
	var m mmAccum
	require.Equal(t, 0.0, m.avg())

	m.add(100)
	m.add(300)
	m.add(200)
	require.Equal(t, 200.0, m.avg())
	require.Equal(t, 100.0, m.minV)
	require.Equal(t, 300.0, m.maxV)
}

func TestCongestionAnalytics_Accumulation(t *testing.T) {
	// the record* / GetCongestionAnalytics methods only touch the analytics fields, so a
	// zero-value StreamAllocator is sufficient to exercise them.
	s := &StreamAllocator{}

	// non-positive samples are ignored
	s.recordBWEEstimate(0)
	s.recordBWEEstimate(-1)
	s.recordTargetBitrate(0)

	s.recordBWEEstimate(1_000_000)
	s.recordBWEEstimate(3_000_000)
	s.recordTargetBitrate(800_000)
	s.recordTargetBitrate(1_200_000)

	ca := s.GetCongestionAnalytics()
	require.EqualValues(t, 2_000_000, ca.BweEstimateAvgBps)
	require.EqualValues(t, 1_000_000, ca.BweEstimateMinBps)
	require.EqualValues(t, 3_000_000, ca.BweEstimateMaxBps)
	require.EqualValues(t, 1_000_000, ca.TargetBitrateAvgBps)
	require.EqualValues(t, 800_000, ca.TargetBitrateMinBps)
	require.EqualValues(t, 1_200_000, ca.TargetBitrateMaxBps)
}

func TestCongestionAnalytics_OveruseUnderuse(t *testing.T) {
	s := &StreamAllocator{}

	// no-op transitions are ignored
	s.recordCongestionTransition(bwe.CongestionStateNone, bwe.CongestionStateNone)

	// toward more congested -> overuse
	s.recordCongestionTransition(bwe.CongestionStateNone, bwe.CongestionStateEarlyWarning)
	s.recordCongestionTransition(bwe.CongestionStateEarlyWarning, bwe.CongestionStateCongested)
	// toward less congested -> underuse
	s.recordCongestionTransition(bwe.CongestionStateCongested, bwe.CongestionStateNone)

	ca := s.GetCongestionAnalytics()
	require.EqualValues(t, 2, ca.OveruseEventCount)
	require.EqualValues(t, 1, ca.UnderuseEventCount)
}
