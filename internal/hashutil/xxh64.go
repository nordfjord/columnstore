package hashutil

import "math/bits"

const (
	xxhPrime1 uint64 = 11400714785074694791
	xxhPrime2 uint64 = 14029467366897019727
	xxhPrime3 uint64 = 1609587929392839161
	xxhPrime4 uint64 = 9650029242287828579
	xxhPrime5 uint64 = 2870177450012600261
)

func XXH64Bytes(input []byte) uint64 {
	n := len(input)
	var h uint64
	p := 0

	if n >= 32 {
		v1 := xxhPrime1
		v1 += xxhPrime2
		v2 := xxhPrime2
		v3 := uint64(0)
		v4 := ^xxhPrime1 + 1

		limit := n - 32
		for p <= limit {
			v1 = xxhRound(v1, readU64(input[p:]))
			v2 = xxhRound(v2, readU64(input[p+8:]))
			v3 = xxhRound(v3, readU64(input[p+16:]))
			v4 = xxhRound(v4, readU64(input[p+24:]))
			p += 32
		}

		h = bits.RotateLeft64(v1, 1) +
			bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) +
			bits.RotateLeft64(v4, 18)
		h = xxhMergeRound(h, v1)
		h = xxhMergeRound(h, v2)
		h = xxhMergeRound(h, v3)
		h = xxhMergeRound(h, v4)
	} else {
		h = xxhPrime5
	}

	h += uint64(n)

	for ; p+8 <= n; p += 8 {
		k1 := xxhRound(0, readU64(input[p:]))
		h ^= k1
		h = bits.RotateLeft64(h, 27)*xxhPrime1 + xxhPrime4
	}
	if p+4 <= n {
		h ^= uint64(readU32(input[p:])) * xxhPrime1
		h = bits.RotateLeft64(h, 23)*xxhPrime2 + xxhPrime3
		p += 4
	}
	for ; p < n; p++ {
		h ^= uint64(input[p]) * xxhPrime5
		h = bits.RotateLeft64(h, 11) * xxhPrime1
	}

	return xxhAvalanche(h)
}

func XXH64String(input string) uint64 {
	n := len(input)
	var h uint64
	p := 0

	if n >= 32 {
		v1 := xxhPrime1
		v1 += xxhPrime2
		v2 := xxhPrime2
		v3 := uint64(0)
		v4 := ^xxhPrime1 + 1

		limit := n - 32
		for p <= limit {
			v1 = xxhRound(v1, readU64String(input[p:]))
			v2 = xxhRound(v2, readU64String(input[p+8:]))
			v3 = xxhRound(v3, readU64String(input[p+16:]))
			v4 = xxhRound(v4, readU64String(input[p+24:]))
			p += 32
		}

		h = bits.RotateLeft64(v1, 1) +
			bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) +
			bits.RotateLeft64(v4, 18)
		h = xxhMergeRound(h, v1)
		h = xxhMergeRound(h, v2)
		h = xxhMergeRound(h, v3)
		h = xxhMergeRound(h, v4)
	} else {
		h = xxhPrime5
	}

	h += uint64(n)

	for ; p+8 <= n; p += 8 {
		k1 := xxhRound(0, readU64String(input[p:]))
		h ^= k1
		h = bits.RotateLeft64(h, 27)*xxhPrime1 + xxhPrime4
	}
	if p+4 <= n {
		h ^= uint64(readU32String(input[p:])) * xxhPrime1
		h = bits.RotateLeft64(h, 23)*xxhPrime2 + xxhPrime3
		p += 4
	}
	for ; p < n; p++ {
		h ^= uint64(input[p]) * xxhPrime5
		h = bits.RotateLeft64(h, 11) * xxhPrime1
	}

	return xxhAvalanche(h)
}

func XXH64TaggedUint64(tag byte, v uint64) uint64 {
	var buf [9]byte
	buf[0] = tag
	writeU64(buf[1:], v)
	return XXH64Bytes(buf[:])
}

func XXH64TaggedBool(tag byte, v bool) uint64 {
	var b byte
	if v {
		b = 1
	}
	return XXH64Bytes([]byte{tag, b})
}

func XXH64TaggedString(tag byte, v string) uint64 {
	var h uint64
	n := len(v) + 1
	p := 0

	if n >= 32 {
		v1 := xxhPrime1
		v1 += xxhPrime2
		v2 := xxhPrime2
		v3 := uint64(0)
		v4 := ^xxhPrime1 + 1

		var chunk [32]byte
		for p+32 <= n {
			fillTaggedChunk(chunk[:], tag, v, p)
			v1 = xxhRound(v1, readU64(chunk[0:]))
			v2 = xxhRound(v2, readU64(chunk[8:]))
			v3 = xxhRound(v3, readU64(chunk[16:]))
			v4 = xxhRound(v4, readU64(chunk[24:]))
			p += 32
		}

		h = bits.RotateLeft64(v1, 1) +
			bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) +
			bits.RotateLeft64(v4, 18)
		h = xxhMergeRound(h, v1)
		h = xxhMergeRound(h, v2)
		h = xxhMergeRound(h, v3)
		h = xxhMergeRound(h, v4)
	} else {
		h = xxhPrime5
	}

	h += uint64(n)

	for ; p+8 <= n; p += 8 {
		var chunk [8]byte
		fillTaggedChunk(chunk[:], tag, v, p)
		k1 := xxhRound(0, readU64(chunk[:]))
		h ^= k1
		h = bits.RotateLeft64(h, 27)*xxhPrime1 + xxhPrime4
	}
	if p+4 <= n {
		var chunk [4]byte
		fillTaggedChunk(chunk[:], tag, v, p)
		h ^= uint64(readU32(chunk[:])) * xxhPrime1
		h = bits.RotateLeft64(h, 23)*xxhPrime2 + xxhPrime3
		p += 4
	}
	for ; p < n; p++ {
		h ^= uint64(taggedStringByte(tag, v, p)) * xxhPrime5
		h = bits.RotateLeft64(h, 11) * xxhPrime1
	}

	return xxhAvalanche(h)
}

func xxhRound(acc, input uint64) uint64 {
	acc += input * xxhPrime2
	acc = bits.RotateLeft64(acc, 31)
	acc *= xxhPrime1
	return acc
}

func xxhMergeRound(acc, val uint64) uint64 {
	acc ^= xxhRound(0, val)
	acc = acc*xxhPrime1 + xxhPrime4
	return acc
}

func xxhAvalanche(h uint64) uint64 {
	h ^= h >> 33
	h *= xxhPrime2
	h ^= h >> 29
	h *= xxhPrime3
	h ^= h >> 32
	return h
}

func readU64(b []byte) uint64 {
	_ = b[7]
	return uint64(b[0]) |
		uint64(b[1])<<8 |
		uint64(b[2])<<16 |
		uint64(b[3])<<24 |
		uint64(b[4])<<32 |
		uint64(b[5])<<40 |
		uint64(b[6])<<48 |
		uint64(b[7])<<56
}

func readU64String(s string) uint64 {
	_ = s[7]
	return uint64(s[0]) |
		uint64(s[1])<<8 |
		uint64(s[2])<<16 |
		uint64(s[3])<<24 |
		uint64(s[4])<<32 |
		uint64(s[5])<<40 |
		uint64(s[6])<<48 |
		uint64(s[7])<<56
}

func readU32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[0]) |
		uint32(b[1])<<8 |
		uint32(b[2])<<16 |
		uint32(b[3])<<24
}

func readU32String(s string) uint32 {
	_ = s[3]
	return uint32(s[0]) |
		uint32(s[1])<<8 |
		uint32(s[2])<<16 |
		uint32(s[3])<<24
}

func writeU64(dst []byte, v uint64) {
	_ = dst[7]
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
	dst[4] = byte(v >> 32)
	dst[5] = byte(v >> 40)
	dst[6] = byte(v >> 48)
	dst[7] = byte(v >> 56)
}

func fillTaggedChunk(dst []byte, tag byte, v string, offset int) {
	for i := range dst {
		dst[i] = taggedStringByte(tag, v, offset+i)
	}
}

func taggedStringByte(tag byte, v string, idx int) byte {
	if idx == 0 {
		return tag
	}
	return v[idx-1]
}
