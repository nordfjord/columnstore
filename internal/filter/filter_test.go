package filter

import (
	"testing"

	"github.com/nordfjord/columnstore/internal/colkind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEqFilterValidate(t *testing.T) {
	require.Error(t, Eq("", "x").Validate())
	require.Error(t, Eq("entity_type", nil).Validate())
	require.NoError(t, Eq("entity_type", "SalesTransaction").Validate())
}

func TestEqFilterString(t *testing.T) {
	row := MapLookup{"entity_type": "SalesTransaction"}

	assert.True(t, Eq("entity_type", "SalesTransaction").Match(row))
	assert.False(t, Eq("entity_type", "Invoice").Match(row))
}

func TestEqFilterNumericStrictTypes(t *testing.T) {
	row := MapLookup{"value": int64(42)}

	assert.True(t, Eq("value", int64(42)).Match(row))
	assert.False(t, Eq("value", 42).Match(row))
	assert.False(t, Eq("value", float64(42)).Match(row))
	assert.False(t, Eq("value", int64(43)).Match(row))
}

func TestEqFilterBool(t *testing.T) {
	row := MapLookup{"is_active": true}

	assert.True(t, Eq("is_active", true).Match(row))
	assert.False(t, Eq("is_active", false).Match(row))
}

func TestEqFilterMissingSemantics(t *testing.T) {
	row := MapLookup{}

	assert.False(t, Eq("missing", nil).Match(row))
	assert.False(t, Eq("missing", "x").Match(row))
}

func TestEqFilterNilValueSemantics(t *testing.T) {
	row := MapLookup{"maybe_nil": nil}

	assert.False(t, Eq("maybe_nil", nil).Match(row))
	assert.False(t, Eq("maybe_nil", "x").Match(row))
}

func TestEqFilterColumns(t *testing.T) {
	assert.Equal(t, []string{"entity_type"}, Eq("entity_type", "x").Columns())
	assert.Nil(t, Eq("", "x").Columns())
}

func TestExistsFilterMatch(t *testing.T) {
	row := MapLookup{"present": "value", "nil_value": nil}

	assert.True(t, Exists("present").Match(row))
	assert.False(t, Exists("nil_value").Match(row))
	assert.False(t, Exists("missing").Match(row))

	assert.False(t, NotExists("present").Match(row))
	assert.True(t, NotExists("nil_value").Match(row))
	assert.True(t, NotExists("missing").Match(row))
}

func TestExistsFilterValidateAndColumns(t *testing.T) {
	assert.Equal(t, []string{"present"}, Exists("present").Columns())
	assert.Equal(t, []string{"present"}, NotExists("present").Columns())
	assert.Nil(t, Exists("").Columns())
	assert.Nil(t, NotExists("").Columns())

	require.NoError(t, Exists("present").Validate())
	require.NoError(t, NotExists("present").Validate())
	require.Error(t, Exists("").Validate())
	require.Error(t, NotExists("").Validate())
}

func TestInAndNotInFilterOptimizeUsesSharedNormalization(t *testing.T) {
	t.Run("in filter normalizes numeric values for float columns", func(t *testing.T) {
		optimized := In("value", int64(10), "x").Optimize(map[string]colkind.ColumnKind{"value": colkind.KindFloat64})
		inFilter, ok := optimized.(InFilter)
		require.True(t, ok)
		assert.Equal(t, []any{float64(10)}, inFilter.Values)
		assert.True(t, In("value", int64(10), "x").CanMatch(map[string]colkind.ColumnKind{"value": colkind.KindFloat64}))
	})

	t.Run("in filter accepts exact float values for int columns", func(t *testing.T) {
		optimized := In("value", float64(10), float64(10.5)).Optimize(map[string]colkind.ColumnKind{"value": colkind.KindInt64})
		inFilter, ok := optimized.(InFilter)
		require.True(t, ok)
		assert.Equal(t, []any{int64(10)}, inFilter.Values)
		assert.True(t, In("value", float64(10)).CanMatch(map[string]colkind.ColumnKind{"value": colkind.KindInt64}))
		assert.False(t, In("value", float64(10.5)).CanMatch(map[string]colkind.ColumnKind{"value": colkind.KindInt64}))
	})

	t.Run("not-in filter normalizes values and still matches missing columns", func(t *testing.T) {
		optimized := NotIn("value", int64(10), "x").Optimize(map[string]colkind.ColumnKind{"value": colkind.KindFloat64})
		notInFilter, ok := optimized.(NotInFilter)
		require.True(t, ok)
		assert.Equal(t, []any{float64(10)}, notInFilter.Values)
		assert.True(t, NotIn("missing", "x").CanMatch(map[string]colkind.ColumnKind{}))
	})
}

func TestNormalizeFilterValueForKind(t *testing.T) {
	t.Run("exact float coerces to int64", func(t *testing.T) {
		value, ok := NormalizeFilterValueForKind(float64(42), colkind.KindInt64)
		require.True(t, ok)
		assert.Equal(t, int64(42), value)
	})

	t.Run("non-integral float does not coerce to int64", func(t *testing.T) {
		_, ok := NormalizeFilterValueForKind(float64(42.5), colkind.KindInt64)
		assert.False(t, ok)
	})

	t.Run("int coerces to float64", func(t *testing.T) {
		value, ok := NormalizeFilterValueForKind(int64(42), colkind.KindFloat64)
		require.True(t, ok)
		assert.Equal(t, float64(42), value)
	})

	t.Run("bool stays bool", func(t *testing.T) {
		value, ok := NormalizeFilterValueForKind(true, colkind.KindBool)
		require.True(t, ok)
		assert.Equal(t, true, value)
	})
}

func TestCompareFilterInt64(t *testing.T) {
	row := MapLookup{"value": int64(42)}
	columns := map[string]colkind.ColumnKind{"value": colkind.KindInt64}
	floatColumns := map[string]colkind.ColumnKind{"value": colkind.KindFloat64}

	assert.True(t, Lt("value", int64(100)).Match(row))
	assert.False(t, Lt("value", int64(42)).Match(row))
	assert.True(t, Gte("value", int64(42)).Match(row))
	assert.False(t, Gte("value", int64(43)).Match(row))

	assert.Equal(t, []string{"value"}, Lt("value", int64(100)).Columns())
	assert.Nil(t, Lt("", int64(100)).Columns())
	assert.True(t, Lt("value", int64(100)).CanMatch(columns))
	assert.True(t, Lt("value", int64(100)).CanMatch(floatColumns))

	require.NoError(t, Lt("value", int64(100)).Validate())
	require.Error(t, Lt("", int64(100)).Validate())
}

func TestCompareFilterOptimizeUsesSharedNormalization(t *testing.T) {
	t.Run("preserves aliases for matching int columns", func(t *testing.T) {
		type score int64

		filter := CompareFilter[score]{
			Type:   FilterOpLt,
			Column: "value",
			Value:  score(10),
			Op:     func(a, b score) bool { return a < b },
		}

		optimized := filter.Optimize(map[string]colkind.ColumnKind{"value": colkind.KindInt64})
		compare, ok := optimized.(CompareFilter[int64])
		require.True(t, ok)
		assert.Equal(t, int64(10), compare.Value)
	})

	t.Run("upcasts int filters for float columns", func(t *testing.T) {
		optimized := Lt("value", int64(10)).Optimize(map[string]colkind.ColumnKind{"value": colkind.KindFloat64})
		compare, ok := optimized.(CompareFilter[float64])
		require.True(t, ok)
		assert.Equal(t, float64(10), compare.Value)
	})

	t.Run("downcasts exact float filters for int columns", func(t *testing.T) {
		optimized := Lt("value", float64(10)).Optimize(map[string]colkind.ColumnKind{"value": colkind.KindInt64})
		compare, ok := optimized.(CompareFilter[int64])
		require.True(t, ok)
		assert.Equal(t, int64(10), compare.Value)
	})

	t.Run("rejects non-integral float filters for int columns", func(t *testing.T) {
		filter := Lt("value", float64(10.5))
		assert.False(t, filter.CanMatch(map[string]colkind.ColumnKind{"value": colkind.KindInt64}))
		assert.IsType(t, filter, filter.Optimize(map[string]colkind.ColumnKind{"value": colkind.KindInt64}))
	})

	t.Run("rejects bool columns", func(t *testing.T) {
		assert.False(t, Lt("value", int64(10)).CanMatch(map[string]colkind.ColumnKind{"value": colkind.KindBool}))
	})
}

func TestAndFilterMatch(t *testing.T) {
	row := MapLookup{"entity_type": "SalesTransaction", "site": "alpha"}

	assert.True(t, And(Eq("entity_type", "SalesTransaction"), Eq("site", "alpha")).Match(row))
	assert.False(t, And(Eq("entity_type", "SalesTransaction"), Eq("site", "beta")).Match(row))
}

func TestOrFilterMatch(t *testing.T) {
	row := MapLookup{"entity_type": "Invoice", "site": "alpha"}

	assert.True(t, Or(Eq("entity_type", "SalesTransaction"), Eq("site", "alpha")).Match(row))
	assert.False(t, Or(Eq("entity_type", "SalesTransaction"), Eq("site", "beta")).Match(row))
}
