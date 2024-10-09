package processhelper

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type contextKey int

const (
	daysLogRetention = 7
	timeFormat       = ""
	TimeStampFormat  = "2006-01-02 15:04:05.000 UTC"
)

const (
	ContextKeyPid contextKey = iota
	ContextKeyPidExternal
)

func TimeBetween(t, min, max time.Time) bool {
	if min.After(max) {
		min, max = max, min
	}

	return (t.Equal(min) || t.After(min)) && (t.Equal(max) || t.Before(max))
}

func ValidateStartFinishInterval(start, finish time.Time) error {
	if start.After(finish) {
		return fmt.Errorf("start is after finish, start: %s, finish: %s", start.Format(TimeStampFormat), finish.Format(TimeStampFormat))
	}

	return nil
}

func Validate7DaysInterval(startAt time.Time) error {
	interval := CurrentTime((-1 * daysLogRetention * 24) * time.Hour)
	if startAt.After(interval) || startAt.Equal(interval) {
		return nil
	}

	return fmt.Errorf(
		"startup is not within the 7 day retention interval, start: %s, interval: %s",
		startAt.String(), interval.String(),
	)
}

func CurrentTime(amountToAdd ...time.Duration) time.Time {
	c := time.Now().UTC()
	for _, a := range amountToAdd {
		c = c.Add(a)
	}

	return c
}

func ValidateTimeZone(dates ...time.Time) error {
	for _, d := range dates {
		if zone, offset := d.Zone(); zone != time.UTC.String() || offset != 0 {
			return fmt.Errorf("the date provided is not in UTC timezone, date: %s", d.String())
		}
	}

	return nil
}

func GetProcessID(ctx context.Context) string {
	return fmt.Sprintf("%s", ctx.Value(ContextKeyPid))
}

func IsRecovery(ctx context.Context) bool {
	result, _ := ctx.Value(ContextKeyPidExternal).(bool)
	return result
}

func FindDateTimeFromLogFile(fileName string) (time.Time, error) {
	regx := regexp.MustCompile(`\d{4}-\d{2}-\d{2}-\d{2,4}`)
	if !regx.MatchString(fileName) {
		return time.Time{}, fmt.Errorf("unable to find the date time for: %s", fileName)
	}

	dateStrings := regx.FindAllString(fileName, -1)
	if size := len(dateStrings); size == 0 || size > 1 {
		return time.Time{}, fmt.Errorf("unable to find the date time for: %s", fileName)
	}

	dateString := dateStrings[0]
	split := make([]int, 0, 5)
	stringDateData := strings.Split(dateString, "-")
	for i, s := range stringDateData {
		if i == 3 {
			hour, _ := strconv.Atoi(s[:2])
			minute, _ := strconv.Atoi(s[2:])
			split = append(split, hour)
			split = append(split, minute)
			break
		}
		num, _ := strconv.Atoi(s)
		split = append(split, num)
	}

	return time.Date(
		split[0], time.Month(split[1]), split[2], split[3], split[4], 0, 0, time.UTC,
	), nil
}

func FormatFileNameForS3(ctx context.Context, dbLogFileName string) (string, error) {
	dateFile, err := FindDateTimeFromLogFile(dbLogFileName)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("rds_log_%s_%d", GetProcessID(ctx), dateFile.UTC().Unix()), nil
}

func CleanTmpFile(f *os.File) {
	if f != nil {
		f.Close()
		os.Remove(f.Name())
	}
}
