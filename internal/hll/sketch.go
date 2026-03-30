package hll

import (
	"fmt"
	"math"
	"math/bits"
)

const (
	defaultPrecision = 14
)

type Sketch struct {
	precision uint8
	registers []uint8
}

func New() *Sketch {
	return NewWithPrecision(defaultPrecision)
}

func NewWithPrecision(p uint8) *Sketch {
	if p < 4 || p > 18 {
		panic(fmt.Sprintf("invalid HLL precision %d", p))
	}
	return &Sketch{
		precision: p,
		registers: make([]uint8, 1<<p),
	}
}

func (s *Sketch) Clone() *Sketch {
	if s == nil {
		return nil
	}
	out := &Sketch{
		precision: s.precision,
		registers: make([]uint8, len(s.registers)),
	}
	copy(out.registers, s.registers)
	return out
}

func (s *Sketch) AddHash(hash uint64) {
	if s == nil {
		return
	}
	hash = mix64(hash)
	idx := hash >> (64 - s.precision)
	w := hash << s.precision
	rank := uint8(bits.LeadingZeros64(w) + 1)
	maxRank := uint8(64 - s.precision + 1)
	if rank > maxRank {
		rank = maxRank
	}
	if rank > s.registers[idx] {
		s.registers[idx] = rank
	}
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func (s *Sketch) Merge(other *Sketch) error {
	if s == nil || other == nil {
		return nil
	}
	if s.precision != other.precision || len(s.registers) != len(other.registers) {
		return fmt.Errorf("hll precision mismatch")
	}
	for i := range s.registers {
		if other.registers[i] > s.registers[i] {
			s.registers[i] = other.registers[i]
		}
	}
	return nil
}

func (s *Sketch) Estimate() float64 {
	if s == nil || len(s.registers) == 0 {
		return 0
	}
	m := float64(len(s.registers))
	sum := 0.0
	zeros := 0
	for _, reg := range s.registers {
		sum += math.Pow(2, -float64(reg))
		if reg == 0 {
			zeros++
		}
	}

	alpha := alphaForM(m)
	estimate := alpha * m * m / sum

	// Linear counting improves small-cardinality accuracy substantially.
	if zeros > 0 {
		linear := m * math.Log(m/float64(zeros))
		threshold := 2.5 * m
		if linear <= threshold {
			return linear
		}
	}

	return estimate
}

func alphaForM(m float64) float64 {
	switch int(m) {
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
