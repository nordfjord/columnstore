package columnstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreQueryFilterMissingColumnBehavior(t *testing.T) {
	store := NewStore(t.TempDir())
	base := time.Unix(1704067200, 0).UTC()

	_, _, err := store.IngestRow("entities", base, map[string]any{"present": int64(1)})
	require.NoError(t, err)
	_, _, err = store.IngestRow("entities", base.Add(time.Minute), map[string]any{"present": int64(2)})
	require.NoError(t, err)

	count := func(t *testing.T, filter Filter) int64 {
		t.Helper()
		res, queryErr := store.Query(t.Context(), Query{
			Start:   base,
			End:     base.Add(time.Hour),
			Dataset: "entities",
			Metric:  Count(),
			Filter:  filter,
		})
		require.NoError(t, queryErr)
		if len(res.Rows) == 0 {
			return 0
		}
		require.Len(t, res.Rows, 1)
		return requireRowInt64(t, res.Rows[0], CountField)
	}

	t.Run("eq missing non-nil returns no rows", func(t *testing.T) {
		assert.Equal(t, int64(0), count(t, Eq("missing", "value")))
	})

	t.Run("not-exists covers former eq nil behavior", func(t *testing.T) {
		assert.Equal(t, int64(2), count(t, NotExists("missing")))
	})

	t.Run("and with missing non-nil branch returns no rows", func(t *testing.T) {
		assert.Equal(t, int64(0), count(t, And(Eq("present", int64(1)), Eq("missing", "x"))))
	})

	t.Run("or with missing branch keeps surviving branch", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, Or(Eq("missing", "x"), Eq("present", int64(1)))))
	})

	t.Run("exists matches present non-nil rows", func(t *testing.T) {
		assert.Equal(t, int64(2), count(t, Exists("present")))
	})

	t.Run("not-exists matches missing rows", func(t *testing.T) {
		assert.Equal(t, int64(2), count(t, NotExists("missing")))
	})
}

func TestStoreQueryFilterTypeCoercion(t *testing.T) {
	store := NewStore(t.TempDir())
	base := time.Unix(1704067200, 0).UTC()

	_, _, err := store.IngestRow("entities", base, map[string]any{"n": int64(42), "f": 1.25})
	require.NoError(t, err)
	_, _, err = store.IngestRow("entities", base.Add(time.Minute), map[string]any{"n": int64(7), "f": 2.5})
	require.NoError(t, err)

	count := func(t *testing.T, filter Filter) int64 {
		t.Helper()
		res, queryErr := store.Query(t.Context(), Query{
			Start:   base,
			End:     base.Add(time.Hour),
			Dataset: "entities",
			Metric:  Count(),
			Filter:  filter,
		})
		require.NoError(t, queryErr)
		if len(res.Rows) == 0 {
			return 0
		}
		require.Len(t, res.Rows, 1)
		return requireRowInt64(t, res.Rows[0], CountField)
	}

	t.Run("int filter coerces to int64 column", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, Eq("n", int(42))))
	})

	t.Run("float32 filter coerces to float64 column", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, Eq("f", float32(1.25))))
	})

	t.Run("non-convertible value does not match", func(t *testing.T) {
		assert.Equal(t, int64(0), count(t, Eq("n", "42")))
	})
}

func TestStoreQueryCompareFilter(t *testing.T) {
	store := NewStore(t.TempDir())
	base := time.Unix(1704067200, 0).UTC()

	_, _, err := store.IngestRow("entities", base, map[string]any{"n": int64(42), "site": "alpha"})
	require.NoError(t, err)
	_, _, err = store.IngestRow("entities", base.Add(time.Minute), map[string]any{"n": int64(7), "site": "beta"})
	require.NoError(t, err)

	count := func(t *testing.T, filter Filter) int64 {
		t.Helper()
		res, queryErr := store.Query(t.Context(), Query{
			Start:   base,
			End:     base.Add(time.Hour),
			Dataset: "entities",
			Metric:  Count(),
			Filter:  filter,
		})
		require.NoError(t, queryErr)
		if len(res.Rows) == 0 {
			return 0
		}
		require.Len(t, res.Rows, 1)
		return requireRowInt64(t, res.Rows[0], CountField)
	}

	t.Run("lt matches rows below threshold", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, Lt("n", int64(10))))
	})

	t.Run("gte matches rows at or above threshold", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, Gte("n", int64(42))))
	})

	t.Run("string compare works lexically", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, Gt("site", "alpha")))
	})
}

func TestStoreQueryReusesColumnForFilterAndMetric(t *testing.T) {
	store := NewStore(t.TempDir())
	base := time.Unix(1704067200, 0).UTC()

	_, _, err := store.IngestRow("entities", base, map[string]any{"n": int64(42)})
	require.NoError(t, err)
	_, _, err = store.IngestRow("entities", base.Add(time.Minute), map[string]any{"n": int64(7)})
	require.NoError(t, err)
	_, _, err = store.IngestRow("entities", base.Add(2*time.Minute), map[string]any{"n": int64(3)})
	require.NoError(t, err)

	res, err := store.Query(t.Context(), Query{
		Start:   base,
		End:     base.Add(time.Hour),
		Dataset: "entities",
		Metric:  Sum("n"),
		Filter:  Lt("n", int64(10)),
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(10), requireRowInt64(t, res.Rows[0], "sum(n)"))
}

func TestStoreQueryInAndNotInFilterTypeCoercion(t *testing.T) {
	store := NewStore(t.TempDir())
	base := time.Unix(1704067200, 0).UTC()

	_, _, err := store.IngestRow("entities", base, map[string]any{"n": int64(42), "f": 1.25})
	require.NoError(t, err)
	_, _, err = store.IngestRow("entities", base.Add(time.Minute), map[string]any{"n": int64(7), "f": 2.5})
	require.NoError(t, err)

	count := func(t *testing.T, filter Filter) int64 {
		t.Helper()
		res, queryErr := store.Query(t.Context(), Query{
			Start:   base,
			End:     base.Add(time.Hour),
			Dataset: "entities",
			Metric:  Count(),
			Filter:  filter,
		})
		require.NoError(t, queryErr)
		if len(res.Rows) == 0 {
			return 0
		}
		require.Len(t, res.Rows, 1)
		return requireRowInt64(t, res.Rows[0], CountField)
	}

	t.Run("in normalizes numeric values for int columns", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, In("n", float64(42), float64(42.5))))
	})

	t.Run("in normalizes numeric values for float columns", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, In("f", float32(1.25), "x")))
	})

	t.Run("not-in ignores non-normalizable values", func(t *testing.T) {
		assert.Equal(t, int64(1), count(t, NotIn("n", float64(42), "x")))
	})

	t.Run("not-in on missing column still matches rows", func(t *testing.T) {
		assert.Equal(t, int64(2), count(t, NotIn("missing", "x")))
	})
}
