package columnstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryResultRowTimeReturnsStoredTime(t *testing.T) {
	expected := time.Date(2024, 1, 15, 9, 30, 0, 0, time.UTC)
	row := NewQueryResultRow(map[string]any{"date": expected})

	require.Equal(t, expected, row.Time("date"))
}

func TestQueryResultRowTimeCoercesRFC3339String(t *testing.T) {
	row := NewQueryResultRow(map[string]any{"occurred_at": "2024-01-15T09:30:00Z"})

	require.Equal(t, time.Date(2024, 1, 15, 9, 30, 0, 0, time.UTC), row.Time("occurred_at"))
}

func TestQueryResultRowTimeCoercesDateOnlyString(t *testing.T) {
	row := NewQueryResultRow(map[string]any{"occurred_at": "2024-01-15"})

	require.Equal(t, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), row.Time("occurred_at"))
}

func TestQueryResultRowTimeCoercesUnixSecondsInt64(t *testing.T) {
	row := NewQueryResultRow(map[string]any{"occurred_at": int64(1705311000)})

	require.Equal(t, time.Unix(1705311000, 0), row.Time("occurred_at"))
}

func TestQueryResultRowTimeCoercesUnixSecondsFloat64(t *testing.T) {
	row := NewQueryResultRow(map[string]any{"occurred_at": float64(1705311000)})

	require.Equal(t, time.Unix(1705311000, 0), row.Time("occurred_at"))
}

func TestQueryResultRowTimeReturnsZeroForUnparseableValue(t *testing.T) {
	row := NewQueryResultRow(map[string]any{"occurred_at": "not-a-time"})

	require.True(t, row.Time("occurred_at").IsZero())
}
