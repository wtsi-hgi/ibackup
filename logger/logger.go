package logger

import (
	"sync"
	"time"
)

// Logger is an optional structured logger used for deep tracing.
//
// It intentionally matches the subset of methods provided by log15.Logger that
// we use for tracing.
//
// Implementations should treat ctx as key/value pairs.
type Logger interface {
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
}

// StartOperation logs "<name> starting" (Info), then periodically logs
// "<name> still running" (Warn) until the returned finish func is called.
//
// This is intended for external interactions that can hang indefinitely.
//
// If l is nil, this becomes a no-op and the returned finish func is also a
// no-op.
func StartOperation(
	l Logger,
	name string,
	stillRunningEvery time.Duration,
	ctx ...interface{},
) (finish func(err error, extraCtx ...interface{})) {
	if l == nil {
		return func(error, ...interface{}) {}
	}

	started := time.Now()

	logStarting(l, name, ctx...)

	done := make(chan struct{})

	var once sync.Once

	startStillRunningTicker(done, l, name, stillRunningEvery, started, ctx)

	return func(err error, extraCtx ...interface{}) {
		once.Do(func() { close(done) })
		logFinished(l, name, started, err, ctx, extraCtx...)
	}
}

func logStarting(l Logger, name string, ctx ...interface{}) {
	l.Info(name+" starting", ctx...)
}

func startStillRunningTicker(
	done <-chan struct{},
	l Logger,
	name string,
	freq time.Duration,
	started time.Time,
	ctx []interface{},
) {
	if freq <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(freq)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				logStillRunning(l, name, started, ctx)
			}
		}
	}()
}

func logStillRunning(l Logger, name string, started time.Time, ctx []interface{}) {
	l.Warn(name+" still running", append(ctx, "elapsed", time.Since(started))...)
}

func logFinished(l Logger, name string, started time.Time, err error, ctx []interface{}, extraCtx ...interface{}) {
	out := make([]interface{}, 0, len(ctx)+4+len(extraCtx))
	out = append(out, ctx...)
	out = append(out, "took", time.Since(started), "err", err)
	out = append(out, extraCtx...)
	l.Info(name+" finished", out...)
}
