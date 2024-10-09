package processhelper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeBetween(t *testing.T) {
	target := time.Now()
	min, max := target.Add(-1*24*time.Hour), target.Add(24*time.Hour)

	data := []struct {
		name     string
		min      time.Time
		max      time.Time
		target   time.Time
		expected bool
		errMsg   string
	}{
		{"time between", min, max, target, true, "the target should be between the interval"},
		{"min-max inverted", max, min, target, true, "the min should invert with the max"},
		{"target < min", min, max, min.Add(-1 * time.Hour), false, fmt.Sprintf("the date '%s' is not on the interval", min.Add(-1*time.Hour).String())},
		{"target > max", min, max, max.Add(2 * time.Hour), false, fmt.Sprintf("the date '%s' is not on the interval", max.Add(2*time.Hour).String())},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result := TimeBetween(d.target, d.min, d.max)
			assert.Equal(t, d.expected, result, d.errMsg)
		})
	}
}

func TestValidateStartFinishInterval(t *testing.T) {
	start := time.Now()
	finish := start.Add(1 * time.Hour)

	data := []struct {
		name     string
		start    time.Time
		finish   time.Time
		expected error
	}{
		{"valid interval", start, finish, nil},
		{
			"invalid interval", finish, start,
			fmt.Errorf(
				"start is after finish, start: %s, finish: %s",
				finish.Format(TimeStampFormat), start.Format(TimeStampFormat),
			),
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			err := ValidateStartFinishInterval(d.start, d.finish)
			if d.expected != nil {
				assert.Error(t, err)
				assert.Equal(t, d.expected.Error(), err.Error())
			}
		})
	}
}

func TestValidate7DaysInterval(t *testing.T) {
	data := []struct {
		name     string
		start    time.Time
		expected error
	}{
		{"between interval", time.Now(), nil},
		{"outside interval", time.Now().Add((-1 * (daysLogRetention + 1) * 24) * time.Hour), errors.New("")},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			err := Validate7DaysInterval(d.start)
			if d.expected != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "startup is not within the 7 day retention interval")
			}
		})
	}
}

func TestCurrentTimeShouldBeUTC(t *testing.T) {
	zone, offset := CurrentTime().Zone()
	assert.Equal(t, zone, time.UTC.String())
	assert.Equal(t, offset, 0)
}

func TestValidateTimeZone(t *testing.T) {
	lc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Error("unable to create the timezone")
	}
	data := []struct {
		name     string
		dates    []time.Time
		expected error
	}{
		{"all dates valid", []time.Time{CurrentTime(5 * time.Hour), CurrentTime(2 * time.Hour)}, nil},
		{"one or multiple invalids", []time.Time{CurrentTime(), time.Now().In(lc), time.Now().In(lc)}, errors.New("")},
		{"empty dates", []time.Time{}, nil},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			err := ValidateTimeZone(d.dates...)
			if d.expected != nil {
				assert.Error(t, err)
				assert.Equal(t, err.Error(), fmt.Sprintf("the date provided is not in UTC timezone, date: %s", d.dates[1]))
			}
		})
	}
}

func TestGetProccessID(t *testing.T) {
	data := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{"with-pid", context.WithValue(context.Background(), ContextKeyPid, "my-pid"), "my-pid"},
		{"without-pid", context.Background(), "%!s(<nil>)"},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			assert.Equal(t, d.expected, GetProcessID(d.ctx))
		})
	}
}

func TestIsRecovery(t *testing.T) {
	data := []struct {
		name     string
		ctx      context.Context
		expected bool
	}{
		{"with-recovery", context.WithValue(context.Background(), ContextKeyPidExternal, true), true},
		{"without-recovery", context.Background(), false},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			assert.Equal(t, d.expected, IsRecovery(d.ctx))
		})
	}
}

func TestFindDateFromLogFile(t *testing.T) {
	data := []struct {
		name     string
		fileName string
		expected time.Time
		err      error
	}{
		{"valid-filename", "error/postgresql.log.2024-02-23-08.csv", time.Date(2024, time.February, 23, 8, 0, 0, 0, time.UTC), nil},
		{"valid-filename-2", "error/postgresql.log.2024-02-23-0830.csv", time.Date(2024, time.February, 23, 8, 30, 0, 0, time.UTC), nil},
		{"invalid-filename", "error/log112349876123", time.Time{}, errors.New("unable to find the date time for: ")},
		{"invalid-filename2", "error/postgresql.log.2024-02-23-08.csv/2024-02-23-08", time.Time{}, errors.New("unable to find the date time for: ")},
		{"invalid-filename3", "error/postgresql.log.asdf-02-23-08.csv", time.Time{}, errors.New("unable to find the date time for: ")},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result, err := FindDateTimeFromLogFile(d.fileName)
			assert.Equal(t, d.expected, result)
			if d.err != nil {
				assert.Error(t, err)
				assert.Equal(t, (d.err.Error() + d.fileName), err.Error())
			}
		})
	}
}

func TestFormatFileNameForS3(t *testing.T) {
	ctx := context.WithValue(context.Background(), ContextKeyPid, "A1234ASDF")
	date := time.Date(2024, time.February, 23, 8, 30, 0, 0, time.UTC)
	data := []struct {
		name     string
		fileName string
		expected string
		err      error
	}{
		{"valid-filename-2", "error/postgresql.log.2024-02-23-0830.csv", fmt.Sprintf("rds_log_%s_%d", GetProcessID(ctx), date.UTC().Unix()), nil},
		{"invalid-filename", "error/log112349876123", "", fmt.Errorf("unable to find the date time for: %s", "error/log112349876123")},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			result, err := FormatFileNameForS3(ctx, d.fileName)
			assert.Equal(t, d.expected, result)
			if d.err != nil {
				assert.Error(t, err)
				assert.Equal(t, d.err.Error(), err.Error())
			}
		})
	}
}

func TestCleanTmpFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("/var/tmp", "go-test-")
	if err != nil {
		t.Error(err.Error())
	}

	_, err = os.Stat(tmpFile.Name())
	assert.Nil(t, err)

	CleanTmpFile(tmpFile)

	_, err = os.Stat(tmpFile.Name())
	assert.True(t, errors.Is(err, os.ErrNotExist))

	// Null Pointer parameter should not panic
	CleanTmpFile(nil)
}
