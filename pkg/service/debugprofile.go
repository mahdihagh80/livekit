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
	"fmt"
	"net/http"
	"runtime"
	"strconv"

	"go.uber.org/atomic"
)

// The mutex and block profiles of /debug/pprof/ are empty unless the runtime
// samples those events, and sampling costs CPU on every contended lock and
// every blocking operation. They are therefore off by default and switched on
// only for the window in which a profile is taken:
//
//	curl 'localhost:<debug port>/debug/pprof/rates?mutex=5&block=100000'
//	curl 'localhost:<debug port>/debug/pprof/mutex?seconds=20' -o mutex.pb.gz
//	curl 'localhost:<debug port>/debug/pprof/block?seconds=20' -o block.pb.gz
//	curl 'localhost:<debug port>/debug/pprof/rates?mutex=0&block=0'
//
// mutex is runtime.SetMutexProfileFraction: on average 1 in N contention
// events is reported, 0 turns it off. block is runtime.SetBlockProfileRate:
// one blocking event is sampled per N nanoseconds spent blocked, 1 records
// every event, 0 turns it off.

// the runtime has no getter for the block profile rate
var debugBlockProfileRate atomic.Int64

type debugProfileRates struct {
	MutexProfileFraction int `json:"mutex_profile_fraction"`
	BlockProfileRate     int `json:"block_profile_rate"`
}

func setDebugProfileRates(mutexFraction, blockRate int) {
	if mutexFraction >= 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
	}
	if blockRate >= 0 {
		runtime.SetBlockProfileRate(blockRate)
		debugBlockProfileRate.Store(int64(blockRate))
	}
}

func currentDebugProfileRates() debugProfileRates {
	return debugProfileRates{
		// a negative value reads the fraction without changing it
		MutexProfileFraction: runtime.SetMutexProfileFraction(-1),
		BlockProfileRate:     int(debugBlockProfileRate.Load()),
	}
}

// debugProfileRatesHandler reports the mutex and block profile sampling rates
// and, when the "mutex" and/or "block" query parameters are given, sets them.
func debugProfileRatesHandler(w http.ResponseWriter, r *http.Request) {
	mutexFraction, blockRate := -1, -1
	q := r.URL.Query()
	for name, dst := range map[string]*int{"mutex": &mutexFraction, "block": &blockRate} {
		v := q.Get(name)
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, fmt.Sprintf("%s must be a non-negative integer", name), http.StatusBadRequest)
			return
		}
		*dst = n
	}
	setDebugProfileRates(mutexFraction, blockRate)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(currentDebugProfileRates())
}
