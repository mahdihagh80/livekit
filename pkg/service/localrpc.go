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
	"errors"
	"fmt"
	"time"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/psrpc"
)

// enableLocalRPC switches this node to the in-process model of config local_rpc (bale
// fork). The signal connection, RoomService, AgentDispatchService and the router's
// CreateRoom call this node directly, and RoomManager registers no per-room or
// per-participant psrpc servers (StartSession, getOrCreateRoom). Nothing on the bus
// can reach the rooms, so it needs a single node without Redis. It must run before
// the server starts serving.
func enableLocalRPC(
	conf *config.Config,
	currentNode routing.LocalNode,
	router routing.Router,
	roomManager *RoomManager,
	roomService livekit.RoomService,
	agentDispatchService *AgentDispatchService,
) error {
	if !conf.LocalRPC.Enabled {
		return nil
	}
	if conf.Redis.IsConfigured() {
		return errors.New("local_rpc requires a single node without redis")
	}

	lr, ok := router.(interface{ SetLocalHandlers(routing.LocalHandlers) })
	if !ok {
		return fmt.Errorf("local_rpc: router %T does not support local handlers", router)
	}
	rs, ok := roomService.(*RoomService)
	if !ok {
		return fmt.Errorf("local_rpc: room service %T is not *RoomService", roomService)
	}

	rs.roomClient = &localRoomClient{rm: roomManager, timeout: conf.LocalRPC.UpdateRoomMetadataTimeout}
	rs.participantClient = &localParticipantClient{rm: roomManager}
	if agentDispatchService != nil {
		agentDispatchService.agentDispatchClient = &localAgentDispatchClient{rm: roomManager}
	}
	lr.SetLocalHandlers(routing.LocalHandlers{
		Session:     &defaultSessionHandler{currentNode: currentNode, router: router, roomManager: roomManager},
		RoomManager: roomManager,
	})

	logger.Infow("local rpc enabled: signal, room service and room handlers in process")
	return nil
}

// call runs a RoomManager method directly, without the caller's cancel, as a psrpc
// server would. These methods return on their own: they do not wait on anything, or,
// for PerformRpc, wait with their own ack and response timers. The psrpc request
// options are therefore not needed. UpdateRoomMetadata is the exception, see
// callWithTimeout.
func call[Req, Res any](ctx context.Context, req Req, fn func(context.Context, Req) (Res, error)) (Res, error) {
	return fn(context.WithoutCancel(ctx), req)
}

// callWithTimeout runs a RoomManager method that may never return, with a timeout (a
// psrpc request timeout option overrides it, 0 is no limit) and the caller's cancel. On
// timeout or cancel the method keeps running, as a psrpc server's handler would.
func callWithTimeout[Req, Res any](
	ctx context.Context,
	timeout time.Duration,
	req Req,
	opts []psrpc.RequestOption,
	fn func(context.Context, Req) (Res, error),
) (Res, error) {
	o := psrpc.RequestOpts{Timeout: timeout}
	for _, opt := range opts {
		opt(&o)
	}

	type result struct {
		res Res
		err error
	}
	resChan := make(chan result, 1)
	go func() {
		res, err := fn(context.WithoutCancel(ctx), req)
		resChan <- result{res, err}
	}()

	var timedOut <-chan time.Time
	if o.Timeout > 0 {
		t := time.NewTimer(o.Timeout)
		defer t.Stop()
		timedOut = t.C
	}

	var zero Res
	select {
	case r := <-resChan:
		return r.res, r.err
	case <-timedOut:
		return zero, psrpc.ErrRequestTimedOut
	case <-ctx.Done():
		return zero, psrpc.ErrRequestCanceled
	}
}

// The adapters below implement the typed psrpc clients by calling RoomManager, which
// holds every room in this mode. They embed no psrpc client, so if an interface gains a
// method, the assignments in enableLocalRPC fail to compile until it is implemented here.

// ------------------------------------------------

type localRoomClient struct {
	rm *RoomManager
	// local_rpc.update_room_metadata_timeout
	timeout time.Duration
}

func (c *localRoomClient) Close() {}

func (c *localRoomClient) DeleteRoom(ctx context.Context, _ rpc.RoomTopic, req *livekit.DeleteRoomRequest, _ ...psrpc.RequestOption) (*livekit.DeleteRoomResponse, error) {
	return call(ctx, req, c.rm.DeleteRoom)
}

func (c *localRoomClient) SendData(ctx context.Context, _ rpc.RoomTopic, req *livekit.SendDataRequest, _ ...psrpc.RequestOption) (*livekit.SendDataResponse, error) {
	return call(ctx, req, c.rm.SendData)
}

// UpdateRoomMetadata waits for the room's update to be applied. If the room closes
// during the call, that never happens, so it has a timeout,
// local_rpc.update_room_metadata_timeout.
func (c *localRoomClient) UpdateRoomMetadata(ctx context.Context, _ rpc.RoomTopic, req *livekit.UpdateRoomMetadataRequest, opts ...psrpc.RequestOption) (*livekit.Room, error) {
	return callWithTimeout(ctx, c.timeout, req, opts, c.rm.UpdateRoomMetadata)
}

func (c *localRoomClient) ListParticipants(ctx context.Context, _ rpc.RoomTopic, req *livekit.ListParticipantsRequest, _ ...psrpc.RequestOption) (*livekit.ListParticipantsResponse, error) {
	return call(ctx, req, c.rm.ListParticipants)
}

func (c *localRoomClient) GetParticipant(ctx context.Context, _ rpc.RoomTopic, req *livekit.RoomParticipantIdentity, _ ...psrpc.RequestOption) (*livekit.ParticipantInfo, error) {
	return call(ctx, req, c.rm.GetParticipant)
}

// ------------------------------------------------

type localParticipantClient struct {
	rm *RoomManager
}

func (c *localParticipantClient) Close() {}

func (c *localParticipantClient) RemoveParticipant(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.RoomParticipantIdentity, _ ...psrpc.RequestOption) (*livekit.RemoveParticipantResponse, error) {
	return call(ctx, req, c.rm.RemoveParticipant)
}

func (c *localParticipantClient) MutePublishedTrack(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.MuteRoomTrackRequest, _ ...psrpc.RequestOption) (*livekit.MuteRoomTrackResponse, error) {
	return call(ctx, req, c.rm.MutePublishedTrack)
}

func (c *localParticipantClient) UpdateParticipant(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.UpdateParticipantRequest, _ ...psrpc.RequestOption) (*livekit.ParticipantInfo, error) {
	return call(ctx, req, c.rm.UpdateParticipant)
}

func (c *localParticipantClient) UpdateSubscriptions(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.UpdateSubscriptionsRequest, _ ...psrpc.RequestOption) (*livekit.UpdateSubscriptionsResponse, error) {
	return call(ctx, req, c.rm.UpdateSubscriptions)
}

func (c *localParticipantClient) ForwardParticipant(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.ForwardParticipantRequest, _ ...psrpc.RequestOption) (*livekit.ForwardParticipantResponse, error) {
	return call(ctx, req, c.rm.ForwardParticipant)
}

func (c *localParticipantClient) MoveParticipant(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.MoveParticipantRequest, _ ...psrpc.RequestOption) (*livekit.MoveParticipantResponse, error) {
	return call(ctx, req, c.rm.MoveParticipant)
}

// PerformRpc is bounded by the participant's own ack and response timers, which use
// the same limit RoomService passes as the psrpc request timeout.
func (c *localParticipantClient) PerformRpc(ctx context.Context, _ rpc.ParticipantTopic, req *livekit.PerformRpcRequest, _ ...psrpc.RequestOption) (*livekit.PerformRpcResponse, error) {
	return call(ctx, req, c.rm.PerformRpc)
}

// ------------------------------------------------

type localAgentDispatchClient struct {
	rm *RoomManager
}

func (c *localAgentDispatchClient) Close() {}

func (c *localAgentDispatchClient) CreateDispatch(ctx context.Context, _ rpc.RoomTopic, req *livekit.AgentDispatch, _ ...psrpc.RequestOption) (*livekit.AgentDispatch, error) {
	return call(ctx, req, c.rm.CreateDispatch)
}

func (c *localAgentDispatchClient) DeleteDispatch(ctx context.Context, _ rpc.RoomTopic, req *livekit.DeleteAgentDispatchRequest, _ ...psrpc.RequestOption) (*livekit.AgentDispatch, error) {
	return call(ctx, req, c.rm.DeleteDispatch)
}

func (c *localAgentDispatchClient) ListDispatch(ctx context.Context, _ rpc.RoomTopic, req *livekit.ListAgentDispatchRequest, _ ...psrpc.RequestOption) (*livekit.ListAgentDispatchResponse, error) {
	return call(ctx, req, c.rm.ListDispatch)
}
