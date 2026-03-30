package timecalc

import "time"

type TimeBucket uint64

const (
	timeBucketMonthShift  = 0
	timeBucketDayShift    = 4
	timeBucketHourShift   = 9
	timeBucketMinuteShift = 14
	timeBucketSecondShift = 20
	timeBucketYearShift   = 26

	timeBucketMonthMask  = 0xF
	timeBucketDayMask    = 0x1F
	timeBucketHourMask   = 0x1F
	timeBucketMinuteMask = 0x3F
	timeBucketSecondMask = 0x3F
	timeBucketYearMask   = 0xFFFF
)

func ResolveTimeZone(name string) *time.Location {
	if name == "" || name == "UTC" {
		return time.UTC
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC
	}
	return loc
}

func ComputeDate(ts int64, timezone, granularity string) time.Time {
	return ComputeDateInLocation(ts, ResolveTimeZone(timezone), granularity)
}

func ComputeTimeBucket(ts int64, timezone, granularity string) TimeBucket {
	loc := ResolveTimeZone(timezone)
	return ComputeTimeBucketInLocation(ts, loc, granularity)
}

func ComputeTimeBucketInLocation(ts int64, loc *time.Location, granularity string) TimeBucket {
	t := time.Unix(ts, 0).In(loc)
	year, month, day := t.Date()
	hour, minute, _ := t.Clock()

	switch granularity {
	case "minute":
		return packTimeBucket(year, month, day, hour, minute, 0)
	case "hour":
		return packTimeBucket(year, month, day, hour, 0, 0)
	case "week":
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		start := t.AddDate(0, 0, -(weekday - 1))
		startYear, startMonth, startDay := start.Date()
		return packTimeBucket(startYear, startMonth, startDay, 0, 0, 0)
	case "month":
		return packTimeBucket(year, month, 1, 0, 0, 0)
	case "day", "":
		fallthrough
	default:
		return packTimeBucket(year, month, day, 0, 0, 0)
	}
}

func ComputeDateInLocation(ts int64, loc *time.Location, granularity string) time.Time {
	return MaterializeTimeBucket(ComputeTimeBucketInLocation(ts, loc, granularity), loc)
}

func MaterializeTimeBucket(bucket TimeBucket, loc *time.Location) time.Time {
	year, month, day, hour, minute, second := UnpackTimeBucket(bucket)
	return time.Date(year, month, day, hour, minute, second, 0, loc)
}

func UnpackTimeBucket(bucket TimeBucket) (int, time.Month, int, int, int, int) {
	v := uint64(bucket)
	year := int((v >> timeBucketYearShift) & timeBucketYearMask)
	month := time.Month((v >> timeBucketMonthShift) & timeBucketMonthMask)
	day := int((v >> timeBucketDayShift) & timeBucketDayMask)
	hour := int((v >> timeBucketHourShift) & timeBucketHourMask)
	minute := int((v >> timeBucketMinuteShift) & timeBucketMinuteMask)
	second := int((v >> timeBucketSecondShift) & timeBucketSecondMask)
	return year, month, day, hour, minute, second
}

func packTimeBucket(year int, month time.Month, day, hour, minute, second int) TimeBucket {
	return TimeBucket(
		(uint64(year&timeBucketYearMask) << timeBucketYearShift) |
			(uint64(second&timeBucketSecondMask) << timeBucketSecondShift) |
			(uint64(minute&timeBucketMinuteMask) << timeBucketMinuteShift) |
			(uint64(hour&timeBucketHourMask) << timeBucketHourShift) |
			(uint64(day&timeBucketDayMask) << timeBucketDayShift) |
			(uint64(month) << timeBucketMonthShift),
	)
}

func ComputeDayOfWeek(ts int64, timezone string) string {
	return ComputeDayOfWeekInLocation(ts, ResolveTimeZone(timezone))
}

func ComputeDayOfWeekInLocation(ts int64, loc *time.Location) string {
	t := time.Unix(ts, 0).In(loc)
	name := t.Weekday().String()
	if len(name) < 3 {
		return name
	}
	return name[:3]
}
