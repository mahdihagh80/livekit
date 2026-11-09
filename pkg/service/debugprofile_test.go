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

package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebugProfileRatesHandler(t *testing.T) {
	prevMutex := runtime.SetMutexProfileFraction(-1)
	t.Cleanup(func() { setDebugProfileRates(prevMutex, 0) })

	call := func(query string) (int, debugProfileRates) {
		rec := httptest.NewRecorder()
		debugProfileRatesHandler(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/rates"+query, nil))
		var got debugProfileRates
		if rec.Code == http.StatusOK {
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		}
		return rec.Code, got
	}

	code, got := call("?mutex=5&block=100000")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, debugProfileRates{MutexProfileFraction: 5, BlockProfileRate: 100000}, got)
	require.Equal(t, 5, runtime.SetMutexProfileFraction(-1))

	// reading does not change anything
	code, got = call("")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, debugProfileRates{MutexProfileFraction: 5, BlockProfileRate: 100000}, got)

	// one parameter leaves the other alone
	code, got = call("?block=0")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, debugProfileRates{MutexProfileFraction: 5, BlockProfileRate: 0}, got)

	code, got = call("?mutex=0")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, debugProfileRates{}, got)

	for _, bad := range []string{"?mutex=-1", "?block=abc", "?mutex=1.5"} {
		code, _ = call(bad)
		require.Equal(t, http.StatusBadRequest, code, bad)
	}
	require.Equal(t, 0, runtime.SetMutexProfileFraction(-1), "a rejected request must not change the rates")
}
