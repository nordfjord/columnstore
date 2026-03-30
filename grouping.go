package columnstore

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	hashutil "github.com/nordfjord/columnstore/internal/hashutil"
	"github.com/nordfjord/columnstore/internal/timecalc"
)

type internedStringValue struct {
	id uint64
}

type dimAtom struct {
	kind uint8
	bits uint64
}

const (
	dimKindNil uint8 = iota
	dimKindInt64
	dimKindFloat64
	dimKindString
	dimKindBool
	dimKindTime
	dimKindFallback
)

const fnv64OffsetBasis = hashutil.FNV64OffsetBasis

// BuildGroupKey serializes dimension values into a deterministic key.
func BuildGroupKey(dimensions []any) string {
	if len(dimensions) == 0 {
		return "__all__"
	}

	var b strings.Builder
	for i, dim := range dimensions {
		if i > 0 {
			b.WriteByte('\x1f')
		}

		switch v := dim.(type) {
		case nil:
			b.WriteString("n")
		case int64:
			b.WriteString("i")
			var numBuf [32]byte
			b.Write(strconv.AppendInt(numBuf[:0], v, 10))
		case float64:
			b.WriteString("f")
			var numBuf [32]byte
			b.Write(strconv.AppendFloat(numBuf[:0], v, 'g', -1, 64))
		case string:
			b.WriteString("s")
			var lenBuf [20]byte
			b.Write(strconv.AppendInt(lenBuf[:0], int64(len(v)), 10))
			b.WriteByte(':')
			b.WriteString(v)
		case bool:
			if v {
				b.WriteString("b1")
			} else {
				b.WriteString("b0")
			}
		case time.Time:
			b.WriteString("t")
			var numBuf [32]byte
			b.Write(strconv.AppendInt(numBuf[:0], v.UnixNano(), 10))
		case timecalc.TimeBucket:
			b.WriteString("c")
			var numBuf [32]byte
			b.Write(strconv.AppendUint(numBuf[:0], uint64(v), 10))
		default:
			j, err := json.Marshal(v)
			if err != nil {
				b.WriteString("u")
				continue
			}
			b.WriteString("j")
			b.Write(j)
		}
	}

	return b.String()
}

func dimAtomsEqual(a, b []dimAtom) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fnv1aAddByte(h uint64, b byte) uint64 {
	return hashutil.FNV1aAddByte(h, b)
}

func fnv1aAddU64(h, v uint64) uint64 {
	return hashutil.FNV1aAddU64(h, v)
}

func internOneDimension(a *MetricAccumulator, v any) dimAtom {
	switch x := v.(type) {
	case nil:
		return dimAtom{kind: dimKindNil}
	case int64:
		return dimAtom{kind: dimKindInt64, bits: uint64(x)}
	case float64:
		return dimAtom{kind: dimKindFloat64, bits: math.Float64bits(x)}
	case string:
		id, ok := a.stringIDs[x]
		if !ok {
			id = a.nextString
			a.nextString++
			a.stringIDs[x] = id
			a.stringByID[id] = x
		}
		return dimAtom{kind: dimKindString, bits: id}
	case internedStringValue:
		return dimAtom{kind: dimKindString, bits: x.id}
	case bool:
		if x {
			return dimAtom{kind: dimKindBool, bits: 1}
		}
		return dimAtom{kind: dimKindBool, bits: 0}
	case time.Time:
		return dimAtom{kind: dimKindTime, bits: uint64(x.UnixNano())}
	case timecalc.TimeBucket:
		return dimAtom{kind: dimKindTime, bits: uint64(x)}
	default:
		key := BuildGroupKey([]any{x})
		id, ok := a.fallbackIDs[key]
		if !ok {
			id = a.nextFallback
			a.nextFallback++
			a.fallbackIDs[key] = id
			a.fallbackByID[id] = x
		}
		return dimAtom{kind: dimKindFallback, bits: id}
	}
}
