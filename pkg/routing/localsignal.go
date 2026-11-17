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

package routing

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/psrpc/pkg/metadata"
)

// LocalSessionHandler is the in-process counterpart of the psrpc signal server: it
// starts the media side of a signal connection. service.SessionHandler satisfies it.
type LocalSessionHandler interface {
	HandleSession(
		ctx context.Context,
		pi ParticipantInit,
		connectionID livekit.ConnectionID,
		requestSource MessageSource,
		responseSink MessageSink,
	) error
}

// LocalRoomManager is the in-process counterpart of the psrpc room manager server.
type LocalRoomManager interface {
	CreateRoom(ctx context.Context, req *livekit.CreateRoomRequest) (*livekit.Room, error)
}

// LocalHandlers are the handlers a router calls directly, without psrpc, when the
// target node is this node. A nil handler keeps the psrpc path for that call.
type LocalHandlers struct {
	Session     LocalSessionHandler
	RoomManager LocalRoomManager
}

// StartLocalParticipantSignal connects a signal connection to a room hosted by this
// process over two message channels, in place of the psrpc stream that
// signalClient.StartParticipantSignal opens. It keeps the behaviour of the relay:
//   - the media side gets its own copy of the participant init, as after the bus;
//   - the session handler runs detached from the caller's context, with the caller's
//     outgoing metadata as its incoming header;
//   - a channel that fills up is closed, like the relay drops a stream whose reader
//     falls behind, which ends the signal connection and makes the client resume;
//   - closing the request sink closes both directions, like closing the stream.
func StartLocalParticipantSignal(
	ctx context.Context,
	roomName livekit.RoomName,
	pi ParticipantInit,
	nodeID livekit.NodeID,
	region string,
	handler LocalSessionHandler,
) (
	connectionID livekit.ConnectionID,
	reqSink MessageSink,
	resSource MessageSource,
	err error,
) {
	connectionID = livekit.ConnectionID(guid.New("CO_"))
	ss, err := pi.ToStartSession(roomName, connectionID)
	if err != nil {
		return
	}
	ss = proto.Clone(ss).(*livekit.StartSession)
	mediaPI, err := ParticipantInitFromStartSession(ss, region)
	if err != nil {
		prometheus.RecordSignalRequestFailure()
		return
	}

	l := utils.GetLogger(ctx).WithValues(
		"room", roomName,
		"reqNodeID", nodeID,
		"participant", pi.Identity,
		"connID", connectionID,
		"participantInit", &pi,
		"startSession", logger.Proto(ss),
	)
	l.Debugw("starting local signal connection")

	reqChan := NewDefaultMessageChannel(connectionID)
	resChan := NewDefaultMessageChannel(connectionID)
	requestSink := &localSignalSink{
		ch:        reqChan,
		peer:      resChan,
		onSuccess: prometheus.RecordSignalRequestSuccess,
		onFailure: prometheus.RecordSignalRequestFailure,
	}
	responseSink := &localSignalSink{
		ch:        resChan,
		onSuccess: prometheus.RecordSignalResponseSuccess,
		onFailure: prometheus.RecordSignalResponseFailure,
	}

	sessionCtx := metadata.NewContextWithIncomingHeader(context.Background(), &metadata.Header{
		RemoteID: string(nodeID),
		SentAt:   time.Now(),
		Metadata: metadata.OutgoingContextMetadata(ctx),
	})
	go func() {
		if err := handler.HandleSession(sessionCtx, *mediaPI, connectionID, reqChan, responseSink); err != nil {
			responseSink.Close()
			l.Errorw("could not handle new participant", err)
		}
	}()

	return connectionID, requestSink, resChan, nil
}

// localSignalSink writes one direction of an in-process signal connection.
type localSignalSink struct {
	ch *MessageChannel
	// peer is the other direction; closing this sink closes it too
	peer      *MessageChannel
	onSuccess func()
	onFailure func()
}

func (s *localSignalSink) WriteMessage(msg proto.Message) error {
	err := s.ch.WriteMessage(msg)
	switch {
	case err == nil:
		s.onSuccess()
	case errors.Is(err, ErrChannelFull):
		s.onFailure()
		s.Close()
	}
	return err
}

func (s *localSignalSink) IsClosed() bool {
	return s.ch.IsClosed()
}

func (s *localSignalSink) Close() {
	s.ch.Close()
	if s.peer != nil {
		s.peer.Close()
	}
}

func (s *localSignalSink) ConnectionID() livekit.ConnectionID {
	return s.ch.ConnectionID()
}
