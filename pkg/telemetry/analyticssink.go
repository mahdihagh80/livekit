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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/hashicorp/go-retryablehttp"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/logger"
)

// analyticsContentType matches the webhook content type so receivers verify the
// signature before parsing the body.
const analyticsContentType = "application/webhook+json"

// ParticipantAnalyticsSink delivers assembled per-participant call analytics to a
// dedicated endpoint, out-of-band from (and independent of) the participant_left
// webhook.
type ParticipantAnalyticsSink interface {
	// Send delivers the analytics asynchronously; it never blocks the caller and
	// is a no-op for a nil payload.
	Send(ctx context.Context, pa *ParticipantAnalytics)
}

type httpAnalyticsSink struct {
	url       string
	apiKey    string
	apiSecret string
	client    *retryablehttp.Client
}

// NewParticipantAnalyticsSink returns a sink that POSTs analytics to url, signed
// with the same scheme as LiveKit webhooks: a base64 sha256 digest of the body
// carried in a short-lived JWT in the Authorization header. It returns nil (a
// disabled sink) unless url, apiKey and apiSecret are all set.
func NewParticipantAnalyticsSink(url, apiKey, apiSecret string) ParticipantAnalyticsSink {
	if url == "" || apiKey == "" || apiSecret == "" {
		return nil
	}
	client := retryablehttp.NewClient()
	client.Logger = nil // avoid noisy default stdout logging
	return &httpAnalyticsSink{
		url:       url,
		apiKey:    apiKey,
		apiSecret: apiSecret,
		client:    client,
	}
}

func (s *httpAnalyticsSink) Send(_ context.Context, pa *ParticipantAnalytics) {
	if s == nil || pa == nil {
		return
	}
	go func() {
		if err := s.post(pa); err != nil {
			logger.Warnw("failed to send participant analytics", err, "participant", pa.ParticipantID)
		}
	}()
}

func (s *httpAnalyticsSink) post(pa *ParticipantAnalytics) error {
	body, err := json.Marshal(pa)
	if err != nil {
		return err
	}

	// sign payload (same scheme as webhook.URLNotifier)
	sum := sha256.Sum256(body)
	b64 := base64.StdEncoding.EncodeToString(sum[:])
	token, err := auth.NewAccessToken(s.apiKey, s.apiSecret).
		SetValidFor(5 * time.Minute).
		SetSha256(b64).
		ToJWT()
	if err != nil {
		return err
	}

	req, err := retryablehttp.NewRequest("POST", s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("content-type", analyticsContentType)

	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	return res.Body.Close()
}
