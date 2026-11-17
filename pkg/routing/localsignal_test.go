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

package routing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc/pkg/metadata"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/routing/routingfakes"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
)

func init() {
	prometheus.Init("node", livekit.NodeType_SERVER)
}

type sessionCall struct {
	ctx          context.Context
	pi           routing.ParticipantInit
	connectionID livekit.ConnectionID
	reqSource    routing.MessageSource
	resSink      routing.MessageSink
}

type testSessionHandler struct {
	calls chan sessionCall
	err   error
}

func newTestSessionHandler(err error) *testSessionHandler {
	return &testSessionHandler{calls: make(chan sessionCall, 1), err: err}
}

func (h *testSessionHandler) HandleSession(
	ctx context.Context,
	pi routing.ParticipantInit,
	connectionID livekit.ConnectionID,
	requestSource routing.MessageSource,
	responseSink routing.MessageSink,
) error {
	h.calls <- sessionCall{ctx, pi, connectionID, requestSource, responseSink}
	return h.err
}

func (h *testSessionHandler) wait(t *testing.T) sessionCall {
	select {
	case c := <-h.calls:
		return c
	case <-time.After(time.Second):
		t.Fatal("session handler not called")
		return sessionCall{}
	}
}

type testRoomManager struct {
	created []*livekit.CreateRoomRequest
}

func (m *testRoomManager) CreateRoom(_ context.Context, req *livekit.CreateRoomRequest) (*livekit.Room, error) {
	m.created = append(m.created, req)
	return &livekit.Room{Name: req.Name}, nil
}

func newTestRouter(t *testing.T) (*routing.LocalRouter, *routing.LocalNodeImpl, *routingfakes.FakeSignalClient, *routingfakes.FakeRoomManagerClient) {
	node, err := routing.NewLocalNode(nil)
	require.NoError(t, err)
	sc := &routingfakes.FakeSignalClient{}
	rmc := &routingfakes.FakeRoomManagerClient{}
	return routing.NewLocalRouter(node, sc, rmc, config.NodeStatsConfig{}), node, sc, rmc
}

func testParticipantInit() routing.ParticipantInit {
	return routing.ParticipantInit{
		Identity: "alice",
		Name:     "Alice",
		Client:   &livekit.ClientInfo{Sdk: livekit.ClientInfo_JS},
		Grants:   &auth.ClaimGrants{Identity: "alice", Video: &auth.VideoGrant{RoomJoin: true, Room: "room1"}},
		CreateRoom: &livekit.CreateRoomRequest{
			Name: "room1",
		},
	}
}

func TestLocalSignal_MessagesBothWays(t *testing.T) {
	router, node, sc, _ := newTestRouter(t)
	h := newTestSessionHandler(nil)
	router.SetLocalHandlers(routing.LocalHandlers{Session: h})

	pi := testParticipantInit()
	ctx := metadata.AppendMetadataToOutgoingContext(context.Background(), "k", "v")
	res, err := router.StartParticipantSignal(ctx, "room1", pi)
	require.NoError(t, err)
	require.Equal(t, 0, sc.StartParticipantSignalCallCount(), "relay must not be used")
	require.Equal(t, node.NodeID(), res.NodeID)

	c := h.wait(t)
	require.Equal(t, res.ConnectionID, c.connectionID)
	require.Equal(t, pi.Identity, c.pi.Identity)
	require.Equal(t, "room1", c.pi.CreateRoom.Name)
	require.Equal(t, "room1", c.pi.Grants.Video.Room)
	// the media side gets its own copy, as after the bus
	require.NotSame(t, pi.Grants, c.pi.Grants)
	require.NotSame(t, pi.Client, c.pi.Client)
	require.NotSame(t, pi.CreateRoom, c.pi.CreateRoom)
	// detached from the caller, with the caller's metadata as incoming header
	require.NoError(t, c.ctx.Err())
	head := metadata.IncomingHeader(c.ctx)
	require.NotNil(t, head)
	require.Equal(t, "v", head.Metadata["k"])

	req := &livekit.SignalRequest{Message: &livekit.SignalRequest_Leave{Leave: &livekit.LeaveRequest{}}}
	require.NoError(t, res.RequestSink.WriteMessage(req))
	require.True(t, proto.Equal(req, <-c.reqSource.ReadChan()))

	resp := &livekit.SignalResponse{Message: &livekit.SignalResponse_Pong{Pong: 1}}
	require.NoError(t, c.resSink.WriteMessage(resp))
	require.True(t, proto.Equal(resp, <-res.ResponseSource.ReadChan()))
}

func TestLocalSignal_RequestSinkCloseClosesBothDirections(t *testing.T) {
	router, _, _, _ := newTestRouter(t)
	h := newTestSessionHandler(nil)
	router.SetLocalHandlers(routing.LocalHandlers{Session: h})

	res, err := router.StartParticipantSignal(context.Background(), "room1", testParticipantInit())
	require.NoError(t, err)
	c := h.wait(t)

	// queued before the close, still delivered after it
	resp := &livekit.SignalResponse{Message: &livekit.SignalResponse_Pong{Pong: 1}}
	require.NoError(t, c.resSink.WriteMessage(resp))

	res.RequestSink.Close()
	require.True(t, res.RequestSink.IsClosed())
	require.True(t, c.reqSource.IsClosed())
	require.True(t, c.resSink.IsClosed())

	msg, ok := <-res.ResponseSource.ReadChan()
	require.True(t, ok)
	require.True(t, proto.Equal(resp, msg))
	_, ok = <-res.ResponseSource.ReadChan()
	require.False(t, ok)

	require.ErrorIs(t, c.resSink.WriteMessage(resp), routing.ErrChannelClosed)
}

func TestLocalSignal_OverflowClosesChannel(t *testing.T) {
	router, _, _, _ := newTestRouter(t)
	h := newTestSessionHandler(nil)
	router.SetLocalHandlers(routing.LocalHandlers{Session: h})

	res, err := router.StartParticipantSignal(context.Background(), "room1", testParticipantInit())
	require.NoError(t, err)
	c := h.wait(t)

	resp := &livekit.SignalResponse{Message: &livekit.SignalResponse_Pong{Pong: 1}}
	for i := 0; i < routing.DefaultMessageChannelSize; i++ {
		require.NoError(t, c.resSink.WriteMessage(resp))
	}
	require.ErrorIs(t, c.resSink.WriteMessage(resp), routing.ErrChannelFull)
	require.True(t, res.ResponseSource.IsClosed())
	// the request direction stays open; the signal connection closes it on its way out
	require.False(t, res.RequestSink.IsClosed())

	n := 0
	for range res.ResponseSource.ReadChan() {
		n++
	}
	require.Equal(t, routing.DefaultMessageChannelSize, n)
}

func TestLocalSignal_HandlerErrorClosesResponses(t *testing.T) {
	router, _, _, _ := newTestRouter(t)
	h := newTestSessionHandler(errors.New("join failed"))
	router.SetLocalHandlers(routing.LocalHandlers{Session: h})

	res, err := router.StartParticipantSignal(context.Background(), "room1", testParticipantInit())
	require.NoError(t, err)
	h.wait(t)

	select {
	case msg := <-res.ResponseSource.ReadChan():
		require.Nil(t, msg)
	case <-time.After(time.Second):
		t.Fatal("response source not closed")
	}
}

func TestLocalSignal_RemoteNodeUsesRelay(t *testing.T) {
	router, _, sc, _ := newTestRouter(t)
	h := newTestSessionHandler(nil)
	router.SetLocalHandlers(routing.LocalHandlers{Session: h})

	_, err := router.StartParticipantSignalWithNodeID(context.Background(), "room1", testParticipantInit(), "ND_other")
	require.NoError(t, err)
	require.Equal(t, 1, sc.StartParticipantSignalCallCount())
	select {
	case <-h.calls:
		t.Fatal("local handler called for another node")
	default:
	}
}

func TestLocalSignal_DisabledUsesRelay(t *testing.T) {
	router, _, sc, _ := newTestRouter(t)

	_, err := router.StartParticipantSignal(context.Background(), "room1", testParticipantInit())
	require.NoError(t, err)
	require.Equal(t, 1, sc.StartParticipantSignalCallCount())
}

func TestLocalCreateRoom(t *testing.T) {
	router, _, _, rmc := newTestRouter(t)

	// not set: psrpc client
	_, err := router.CreateRoom(context.Background(), &livekit.CreateRoomRequest{Name: "room1"})
	require.NoError(t, err)
	require.Equal(t, 1, rmc.CreateRoomCallCount())

	rm := &testRoomManager{}
	router.SetLocalHandlers(routing.LocalHandlers{RoomManager: rm})
	room, err := router.CreateRoom(context.Background(), &livekit.CreateRoomRequest{Name: "room2"})
	require.NoError(t, err)
	require.Equal(t, "room2", room.Name)
	require.Len(t, rm.created, 1)
	require.Equal(t, 1, rmc.CreateRoomCallCount())

	// another node: psrpc client
	_, err = router.CreateRoomWithNodeID(context.Background(), &livekit.CreateRoomRequest{Name: "room3"}, "ND_other")
	require.NoError(t, err)
	require.Equal(t, 2, rmc.CreateRoomCallCount())
	require.Len(t, rm.created, 1)
}
