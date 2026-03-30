package hashutil

const (
	FNV64OffsetBasis = uint64(14695981039346656037)
	FNV64Prime       = uint64(1099511628211)
)

func FNV1aAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= FNV64Prime
	return h
}

func FNV1aAddU64(h, v uint64) uint64 {
	h = FNV1aAddByte(h, byte(v))
	h = FNV1aAddByte(h, byte(v>>8))
	h = FNV1aAddByte(h, byte(v>>16))
	h = FNV1aAddByte(h, byte(v>>24))
	h = FNV1aAddByte(h, byte(v>>32))
	h = FNV1aAddByte(h, byte(v>>40))
	h = FNV1aAddByte(h, byte(v>>48))
	h = FNV1aAddByte(h, byte(v>>56))
	return h
}
