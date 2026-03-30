package columnstore

import "github.com/nordfjord/columnstore/internal/filter"

type Filter = filter.Filter

func Eq(column string, value any) filter.Filter {
	return filter.Eq(column, value)
}

func Exists(column string) filter.Filter {
	return filter.Exists(column)
}

func NotExists(column string) filter.Filter {
	return filter.NotExists(column)
}

func In(column string, values ...any) filter.Filter {
	return filter.In(column, values...)
}

func NotIn(column string, values ...any) filter.Filter {
	return filter.NotIn(column, values...)
}

func Lt[T filter.Ordered](column string, value T) filter.Filter {
	return filter.Lt[T](column, value)
}

func Lte[T filter.Ordered](column string, value T) filter.Filter {
	return filter.Lte[T](column, value)
}

func Gt[T filter.Ordered](column string, value T) filter.Filter {
	return filter.Gt[T](column, value)
}

func Gte[T filter.Ordered](column string, value T) filter.Filter {
	return filter.Gte[T](column, value)
}

func And(exprs ...filter.Filter) filter.Filter {
	return filter.And(exprs...)
}

func Or(exprs ...filter.Filter) filter.Filter {
	return filter.Or(exprs...)
}
