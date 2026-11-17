// Copyright 2023 LiveKit, Inc.
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

package routing

import (
	"context"
	"time"

	"go.uber.org/atomic"

	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

var _ Router = (*LocalRouter)(nil)

// a router of messages on the same node, basic implementation for local testing
type LocalRouter struct {
	currentNode       LocalNode
	signalClient      SignalClient
	roomManagerClient RoomManagerClient
	nodeStatsConfig   config.NodeStatsConfig

	// channels for each participant
	requestChannels  map[string]*MessageChannel
	responseChannels map[string]*MessageChannel
	isStarted        atomic.Bool

	// handlers called in process when the target node is this node (bale fork)
	local atomic.Pointer[LocalHandlers]
}

func NewLocalRouter(
	currentNode LocalNode,
	signalClient SignalClient,
	roomManagerClient RoomManagerClient,
	nodeStatsConfig config.NodeStatsConfig,
) *LocalRouter {
	return &LocalRouter{
		currentNode:       currentNode,
		signalClient:      signalClient,
		roomManagerClient: roomManagerClient,
		nodeStatsConfig:   nodeStatsConfig,
		requestChannels:   make(map[string]*MessageChannel),
		responseChannels:  make(map[string]*MessageChannel),
	}
}

func (r *LocalRouter) GetNodeForRoom(_ context.Context, _ livekit.RoomName) (*livekit.Node, error) {
	return r.currentNode.Clone(), nil
}

func (r *LocalRouter) SetNodeForRoom(_ context.Context, _ livekit.RoomName, _ livekit.NodeID) error {
	return nil
}

func (r *LocalRouter) ClearRoomState(_ context.Context, _ livekit.RoomName) error {
	return nil
}

func (r *LocalRouter) RegisterNode() error {
	return nil
}

func (r *LocalRouter) UnregisterNode() error {
	return nil
}

func (r *LocalRouter) RemoveDeadNodes() error {
	return nil
}

func (r *LocalRouter) GetNode(nodeID livekit.NodeID) (*livekit.Node, error) {
	if nodeID == r.currentNode.NodeID() {
		return r.currentNode.Clone(), nil
	}
	return nil, ErrNotFound
}

func (r *LocalRouter) ListNodes() ([]*livekit.Node, error) {
	return []*livekit.Node{
		r.currentNode.Clone(),
	}, nil
}

func (r *LocalRouter) CreateRoom(ctx context.Context, req *livekit.CreateRoomRequest) (res *livekit.Room, err error) {
	return r.CreateRoomWithNodeID(ctx, req, r.currentNode.NodeID())
}

func (r *LocalRouter) CreateRoomWithNodeID(ctx context.Context, req *livekit.CreateRoomRequest, nodeID livekit.NodeID) (res *livekit.Room, err error) {
	if h := r.local.Load(); h != nil && h.RoomManager != nil && nodeID == r.currentNode.NodeID() {
		return h.RoomManager.CreateRoom(ctx, req)
	}
	return r.roomManagerClient.CreateRoom(ctx, nodeID, req)
}

// SetLocalHandlers makes calls to this node skip psrpc (bale fork, config local_rpc).
// Set it before the server starts serving.
func (r *LocalRouter) SetLocalHandlers(h LocalHandlers) {
	r.local.Store(&h)
}

func (r *LocalRouter) StartParticipantSignal(ctx context.Context, roomName livekit.RoomName, pi ParticipantInit) (res StartParticipantSignalResults, err error) {
	return r.StartParticipantSignalWithNodeID(ctx, roomName, pi, r.currentNode.NodeID())
}

func (r *LocalRouter) StartParticipantSignalWithNodeID(ctx context.Context, roomName livekit.RoomName, pi ParticipantInit, nodeID livekit.NodeID) (res StartParticipantSignalResults, err error) {
	var connectionID livekit.ConnectionID
	var reqSink MessageSink
	var resSource MessageSource
	if h := r.local.Load(); h != nil && h.Session != nil && nodeID == r.currentNode.NodeID() {
		connectionID, reqSink, resSource, err = StartLocalParticipantSignal(ctx, roomName, pi, nodeID, r.currentNode.Region(), h.Session)
	} else {
		connectionID, reqSink, resSource, err = r.signalClient.StartParticipantSignal(ctx, roomName, pi, nodeID)
	}
	if err != nil {
		logger.Errorw(
			"could not handle new participant", err,
			"room", roomName,
			"participant", pi.Identity,
			"connID", connectionID,
		)
	} else {
		return StartParticipantSignalResults{
			ConnectionID:   connectionID,
			RequestSink:    reqSink,
			ResponseSource: resSource,
			NodeID:         nodeID,
		}, nil
	}
	return
}

func (r *LocalRouter) Start() error {
	if r.isStarted.Swap(true) {
		return nil
	}
	go r.statsWorker()
	// go r.memStatsWorker()
	return nil
}

func (r *LocalRouter) Drain() {
	r.currentNode.SetState(livekit.NodeState_SHUTTING_DOWN)
}

func (r *LocalRouter) Stop() {}

func (r *LocalRouter) GetRegion() string {
	return r.currentNode.Region()
}

func (r *LocalRouter) statsWorker() {
	for {
		if !r.isStarted.Load() {
			return
		}
		<-time.After(r.nodeStatsConfig.StatsUpdateInterval)
		r.currentNode.UpdateNodeStats()
	}
}

/*
	func (r *LocalRouter) memStatsWorker() {
		ticker := time.NewTicker(time.Second * 30)
		defer ticker.Stop()

		for {
			<-ticker.C

			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			logger.Infow("memstats",
				"mallocs", m.Mallocs, "frees", m.Frees, "m-f", m.Mallocs-m.Frees,
				"hinuse", m.HeapInuse, "halloc", m.HeapAlloc, "frag", m.HeapInuse-m.HeapAlloc,
			)
		}
	}
*/
