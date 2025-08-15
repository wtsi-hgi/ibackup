/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package metadata

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/tasks"
)

const (
	hoursInDay  = 24
	hoursInWeek = hoursInDay * 7

	validMetaParts       = 2
	validMetaKeyDividers = 2
)

var (
	ErrInvalidDurationFormat   = errors.New("duration must be in the form <number><unit>, or YYYY-MM-DD")
	ErrInvalidReviewRemoveDate = errors.New("--review duration must be smaller than --removal duration")
	errInvalidMetaLength       = errors.New("meta must be provided in the form key=value")
	errInvalidMetaNamespace    = errors.New("namespace is incorrect, must be 'ibackup:user:' or empty")
)

// ParseMetadataDates parses the review and removal dates from the given
// strings, defaulting to values determine by the reason if the entries are
// blank.
func ParseMetadataDates(reason db.Reason, review, removal string) (time.Time, time.Time, error) {
	if reason == db.Unset {
		reason = db.Backup
	}

	review, removal = setReviewAndRemovalDurations(reason, review, removal)

	removalDate, err := getFutureDateFromDurationOrDate(removal)
	if err != nil {
		return time.Time{}, removalDate, err
	}

	reviewDate, err := getFutureDateFromDurationOrDate(review)
	if err != nil {
		return reviewDate, removalDate, err
	}

	if reviewDate.After(removalDate) {
		return reviewDate, removalDate,
			fmt.Errorf("invalid meta: '%s is after %s' %w", review, removal, ErrInvalidReviewRemoveDate)
	}

	return reviewDate, removalDate, nil
}

// setReviewAndRemovalDurations returns the review and removal durations for a
// given reason. If the user has not set a duration, the function will return
// the default corresponding to the provided reason.
func setReviewAndRemovalDurations(reason db.Reason, review, removal string) (string, string) {
	defaultReviewDuration, defaultRemovalDuration := getDefaultReviewAndRemovalDurations(reason)
	if review == "" {
		review = defaultReviewDuration
	}

	if removal == "" {
		removal = defaultRemovalDuration
	}

	return review, removal
}

func getDefaultReviewAndRemovalDurations(reason db.Reason) (string, string) {
	switch reason {
	case db.Archive:
		return "1y", "2y"
	case db.Quarantine:
		return "2m", "3m"
	default:
		return "6m", "1y"
	}
}

// getFutureDateFromDurationOrDate calculates the future date based on the time
// string provided. Returns an error if duration is not in the format
// '<number><unit>', e.g. '1y', '12m' or YYYY-MM-DD.
func getFutureDateFromDurationOrDate(t string) (time.Time, error) {
	date, err := time.Parse("2006-01-02", t)
	if err == nil {
		return date, nil
	}

	num, err := strconv.Atoi(t[:len(t)-1])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid meta: '%s' %w", t, ErrInvalidDurationFormat)
	}

	switch t[len(t)-1] {
	case 'y':
		return time.Now().AddDate(num, 0, 0), nil
	case 'm':
		return time.Now().AddDate(0, num, 0), nil
	default:
		return time.Time{}, fmt.Errorf("invalid meta: '%s' %w", t, ErrInvalidDurationFormat)
	}
}

func ParseDuration(s string, minDuration time.Duration) (time.Duration, error) {
	s = convertDurationString(s)

	monitorDuration, err := time.ParseDuration(s)
	if err != nil {
		return 0, &InvalidMonitorDurationError{
			Err: err,
		}
	}

	if monitorDuration < minDuration {
		return 0, &MonitorDurationTooShortError{
			Duration:    monitorDuration,
			MinDuration: minDuration,
		}
	}

	return monitorDuration, nil
}

func convertDurationString(s string) string {
	durationRegex := regexp.MustCompile("[0-9]+[dw]")

	return durationRegex.ReplaceAllStringFunc(s, func(d string) string {
		num, err := strconv.ParseInt(d[:len(d)-1], 10, 64)
		if err != nil {
			return d
		}

		switch d[len(d)-1] {
		case 'd':
			num *= hoursInDay
		case 'w':
			num *= hoursInWeek
		}

		return strconv.FormatInt(num, 10) + "h"
	})
}

// ParseMetaString takes a string of key=value pairs and map of existing
// metadata and returns a Meta. If the metadata string is nonblank and valid, it
// will be used to populate the Meta, otherwise the existing meta will be used.
func ParseMetaString(meta string) (map[string]string, error) {
	kvs := strings.Split(meta, ";")
	mm := make(map[string]string, len(kvs))

	if meta == "" {
		return nil, nil //nolint:nilnil
	}

	for _, kv := range kvs {
		key, value, err := validateAndCreateUserMetadata(kv)
		if err != nil {
			return nil, fmt.Errorf("invalid meta: '%s' %w", kv, err)
		}

		mm[key] = value
	}

	return mm, nil
}

// validateAndCreateUserMetadata takes a key=value string, validates it as a
// metadata value then returns the key prefixed with the user namespace,
// 'ibackup:user:', and the value. Returns an error if the meta is invalid.
func validateAndCreateUserMetadata(kv string) (string, string, error) {
	parts := strings.Split(kv, "=")
	if len(parts) != validMetaParts {
		return "", "", fmt.Errorf("invalid meta: '%s' %w", kv, errInvalidMetaLength)
	}

	key, err := handleNamespace(parts[0])
	value := parts[1]

	return key, value, err
}

// handleNamespace prefixes the user namespace 'ibackup:user:' onto the key if
// it isn't already included. Returns an error if the key contains an invalid
// namespace.
func handleNamespace(key string) (string, error) {
	keyDividers := strings.Count(key, ":")

	switch {
	case keyDividers == 0:
		return key, nil
	case keyDividers != validMetaKeyDividers:
		return "", fmt.Errorf("invalid meta: '%s' %w", key, errInvalidMetaNamespace)
	case strings.HasPrefix(key, tasks.MetaUserNamespace):
		return strings.TrimPrefix(key, tasks.MetaUserNamespace), nil
	default:
		return "", fmt.Errorf("invalid meta: '%s' %w", key, errInvalidMetaNamespace)
	}
}
