// Copyright 2026 LiveKit, Inc.
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

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/rtc"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc"
)

func TestLocalRPCCall_Direct(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := &livekit.ListParticipantsRequest{Room: "room1"}
	res, err := call(ctx, req, func(lctx context.Context, r *livekit.ListParticipantsRequest) (string, error) {
		// like a psrpc server, the method does not see the caller's cancel
		require.NoError(t, lctx.Err())
		return r.Room, nil
	})
	require.NoError(t, err)
	require.Equal(t, "room1", res)
}

func TestLocalRPCCallWithTimeout_Result(t *testing.T) {
	req := &livekit.UpdateRoomMetadataRequest{Room: "room1"}
	res, err := callWithTimeout(context.Background(), time.Second, req, nil,
		func(_ context.Context, r *livekit.UpdateRoomMetadataRequest) (string, error) {
			return r.Room, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, "room1", res)
}

func TestLocalRPCCallWithTimeout_Timeout(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	blocking := func(context.Context, *livekit.UpdateRoomMetadataRequest) (string, error) {
		<-block
		return "", nil
	}
	req := &livekit.UpdateRoomMetadataRequest{Room: "room1"}

	t.Run("default", func(t *testing.T) {
		_, err := callWithTimeout(context.Background(), 20*time.Millisecond, req, nil, blocking)
		require.ErrorIs(t, err, psrpc.ErrRequestTimedOut)
	})

	t.Run("request option", func(t *testing.T) {
		start := time.Now()
		_, err := callWithTimeout(context.Background(), time.Hour, req,
			[]psrpc.RequestOption{psrpc.WithRequestTimeout(20 * time.Millisecond)}, blocking)
		require.ErrorIs(t, err, psrpc.ErrRequestTimedOut)
		require.Less(t, time.Since(start), 5*time.Second)
	})
}

func TestLocalRPCCallWithTimeout_CancelDetached(t *testing.T) {
	req := &livekit.UpdateRoomMetadataRequest{Room: "room1"}
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan context.Context, 1)
	finish := make(chan struct{})
	errChan := make(chan error, 1)
	go func() {
		_, err := callWithTimeout(ctx, time.Hour, req, nil,
			func(lctx context.Context, _ *livekit.UpdateRoomMetadataRequest) (string, error) {
				started <- lctx
				<-finish
				return "", nil
			})
		errChan <- err
	}()

	lctx := <-started
	cancel()
	require.ErrorIs(t, <-errChan, psrpc.ErrRequestCanceled)
	// like a psrpc server, the handler is not cancelled with the caller
	require.NoError(t, lctx.Err())
	close(finish)
}

func TestLocalRoomClient_RoomNotFound(t *testing.T) {
	rm := &RoomManager{rooms: make(map[livekit.RoomName]*rtc.Room)}
	c := &localRoomClient{rm: rm, timeout: time.Second}
	_, err := c.ListParticipants(context.Background(), "room1", &livekit.ListParticipantsRequest{Room: "room1"})
	require.ErrorIs(t, err, ErrRoomNotFound)
}

type handlerRouter struct {
	*routingfakes.FakeRouter
	handlers *routing.LocalHandlers
}

func (r *handlerRouter) SetLocalHandlers(h routing.LocalHandlers) {
	r.handlers = &h
}

func TestEnableLocalRPC_On(t *testing.T) {
	router := &handlerRouter{FakeRouter: &routingfakes.FakeRouter{}}
	rm := &RoomManager{}
	rs := &RoomService{}
	ads := &AgentDispatchService{}
	require.NoError(t, enableLocalRPC(&config.Config{LocalRPC: config.LocalRPCConfig{Enabled: true}}, nil, router, rm, rs, ads))

	require.NotNil(t, router.handlers)
	require.NotNil(t, router.handlers.Session)
	require.Same(t, rm, router.handlers.RoomManager)
	require.IsType(t, &localRoomClient{}, rs.roomClient)
	require.IsType(t, &localParticipantClient{}, rs.participantClient)
	require.IsType(t, &localAgentDispatchClient{}, ads.agentDispatchClient)
}

func TestEnableLocalRPC_RequiresNoRedis(t *testing.T) {
	conf := &config.Config{LocalRPC: config.LocalRPCConfig{Enabled: true}}
	conf.Redis.Address = "127.0.0.1:6379"
	require.ErrorContains(t, enableLocalRPC(conf, nil, nil, nil, nil, nil), "without redis")
}

func TestEnableLocalRPC_Off(t *testing.T) {
	router := &handlerRouter{FakeRouter: &routingfakes.FakeRouter{}}
	rs := &RoomService{}
	require.NoError(t, enableLocalRPC(&config.Config{}, nil, router, nil, rs, nil))
	require.Nil(t, router.handlers)
	require.Nil(t, rs.roomClient)
}
