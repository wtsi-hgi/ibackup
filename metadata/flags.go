package metadata

import (
	"errors"
	"fmt"
	"time"

	"github.com/wtsi-hgi/ibackup/db"
)

var (
	ErrInvalidReason = errors.New("reason must be 'backup', 'archive', 'quarantine'")
)

type Reason db.Reason

func (r *Reason) Set(value string) error {
	switch value {
	case "unset":
		*r = Reason(db.Unset)
	case "backup":
		*r = Reason(db.Backup)
	case "archive":
		*r = Reason(db.Archive)
	case "quarantine":
		*r = Reason(db.Quarantine)
	default:
		return ErrInvalidReason
	}

	return nil
}

func (r Reason) String() string {
	switch r {
	case Reason(db.Unset):
		return "unset"
	case Reason(db.Backup):
		return "backup"
	case Reason(db.Archive):
		return "archive"
	case Reason(db.Quarantine):
		return "quarantine"
	default:
		return "unknown"
	}
}

func (r Reason) Type() string {
	return "Reason"
}

func (r Reason) AsReason() db.Reason {
	return db.Reason(r)
}

type InvalidMonitorDurationError struct {
	Err error
}

func (e *InvalidMonitorDurationError) Error() string {
	return fmt.Sprintf("invalid monitor duration: %v", e.Err)
}

type MonitorDurationTooShortError struct {
	Duration    time.Duration
	MinDuration time.Duration
}

func (e *MonitorDurationTooShortError) Error() string {
	return fmt.Sprintf("monitor duration must be %s or more, not %s", e.MinDuration, e.Duration)
}
