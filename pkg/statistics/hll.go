// Copyright 2026 PingCAP, Inc.
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

package statistics

import (
	"hash"
	"math"
	"math/bits"
)

const defaultHLLPrecision = uint8(14)

// HyperLogLog is a probabilistic cardinality estimator.
// It is used only during build to estimate NDV from samples.
type HyperLogLog struct {
	p         uint8
	registers []uint8
}

// NewHyperLogLog creates a new HyperLogLog with the given precision.
func NewHyperLogLog(p uint8) *HyperLogLog {
	if p < 4 {
		p = 4
	} else if p > 18 {
		p = 18
	}
	return &HyperLogLog{
		p:         p,
		registers: make([]uint8, 1<<p),
	}
}

// InsertBytes adds a value into the sketch.
func (h *HyperLogLog) InsertBytes(b []byte) {
	if h == nil {
		return
	}
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	_, _ = hashFunc.Write(b)
	h.insertHash(hashFunc.Sum64())
	murmur3Pool.Put(hashFunc)
}

// Estimate returns the estimated cardinality.
func (h *HyperLogLog) Estimate() int64 {
	if h == nil || len(h.registers) == 0 {
		return 0
	}
	m := float64(len(h.registers))
	alpha := alphaForHLL(m)
	sum := 0.0
	zeros := 0
	for _, r := range h.registers {
		sum += math.Ldexp(1.0, -int(r))
		if r == 0 {
			zeros++
		}
	}
	estimate := alpha * m * m / sum
	// Small range correction (linear counting)
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}
	return int64(math.Round(estimate))
}

func (h *HyperLogLog) insertHash(x uint64) {
	shift := uint(64 - h.p)
	idx := int(x >> shift)
	w := x << shift
	rank := uint8(bits.LeadingZeros64(w) + 1)
	maxRank := uint8(64 - h.p + 1)
	if rank > maxRank {
		rank = maxRank
	}
	if rank > h.registers[idx] {
		h.registers[idx] = rank
	}
}

func alphaForHLL(m float64) float64 {
	switch m {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1 + 1.079/m)
	}
}
