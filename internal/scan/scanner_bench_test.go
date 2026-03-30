package scan

import (
	"bytes"
	"fmt"
	"testing"

	format "github.com/nordfjord/columnstore/internal/format"
)

func BenchmarkStringScannerNext(b *testing.B) {
	benchmarks := []struct {
		name     string
		rows     int
		distinct int
	}{
		{name: "repeated_4096", rows: 4096, distinct: 16},
		{name: "unique_4096", rows: 4096, distinct: 4096},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			payload := buildStringTuplePayload(b, bm.rows, bm.distinct)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()

			for b.Loop() {
				scanner := NewStringScanner(bytes.NewReader(payload))
				rows := 0
				for {
					ok, err := scanner.Next()
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						break
					}
					rows++
				}
				if rows != bm.rows {
					b.Fatalf("read %d rows, want %d", rows, bm.rows)
				}
			}
		})
	}
}

func buildStringTuplePayload(b *testing.B, rows, distinct int) []byte {
	b.Helper()
	var buf bytes.Buffer
	writer := format.NewStringTupleWriter(&buf)
	for i := range rows {
		value := fmt.Sprintf("value-%06d", i%distinct)
		if err := writer.Write(int32(i), value); err != nil {
			b.Fatalf("write tuple %d: %v", i, err)
		}
	}
	return buf.Bytes()
}
