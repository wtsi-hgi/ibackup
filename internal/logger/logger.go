/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors: Sendu Bala <sb10@sanger.ac.uk>
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
	Debug(msg string, ctx ...any)
	Info(msg string, ctx ...any)
	Warn(msg string, ctx ...any)
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
	ctx ...any,
) (finish func(err error, extraCtx ...any)) {
	if l == nil {
		return func(error, ...any) {}
	}

	started := time.Now()

	logStarting(l, name, ctx...)

	done := make(chan struct{})

	var once sync.Once

	startStillRunningTicker(done, l, name, stillRunningEvery, started, ctx)

	return func(err error, extraCtx ...any) {
		once.Do(func() { close(done) })
		logFinished(l, name, started, err, ctx, extraCtx...)
	}
}

func logStarting(l Logger, name string, ctx ...any) {
	l.Info(name+" starting", ctx...)
}

func startStillRunningTicker(
	done <-chan struct{},
	l Logger,
	name string,
	freq time.Duration,
	started time.Time,
	ctx []any,
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

func logStillRunning(l Logger, name string, started time.Time, ctx []any) {
	l.Warn(name+" still running", append(ctx, "elapsed", time.Since(started))...)
}

func logFinished(l Logger, name string, started time.Time, err error, ctx []any, extraCtx ...any) {
	out := make([]any, 0, len(ctx)+4+len(extraCtx))
	out = append(out, ctx...)
	out = append(out, "took", time.Since(started), "err", err)
	out = append(out, extraCtx...)
	l.Info(name+" finished", out...)
}

// PrintfLogger is a minimal printf-style logger.
//
// *log.Logger satisfies this interface.
type PrintfLogger interface {
	Printf(format string, a ...any)
}

// LogIfBlocked logs a warning if the caller is still blocked after firstAfter,
// then continues warning every until done is closed.
//
// This is intended for thread/goroutine blocking on external interactions.
func LogIfBlocked(
	done <-chan struct{},
	l PrintfLogger,
	trace string,
	op string,
	firstAfter time.Duration,
	every time.Duration,
	details func() string,
) {
	if done == nil || l == nil {
		return
	}

	if details == nil {
		details = func() string { return "" }
	}

	if every <= 0 {
		logIfBlockedOnce(done, l, trace, op, firstAfter, details)

		return
	}

	logIfBlockedRepeating(done, l, trace, op, firstAfter, every, details)
}

func logIfBlockedOnce(
	done <-chan struct{},
	l PrintfLogger,
	trace string,
	op string,
	firstAfter time.Duration,
	details func() string,
) {
	firstTimer := time.NewTimer(firstAfter)
	defer firstTimer.Stop()

	select {
	case <-done:
		return
	case <-firstTimer.C:
		l.Printf("[%s] WARNING: %s still blocked after %s (%s)", trace, op, firstAfter, details())
		<-done
	}
}

func logIfBlockedRepeating(done <-chan struct{}, l PrintfLogger, trace, op string,
	firstAfter, every time.Duration, details func() string,
) {
	firstTimer := time.NewTimer(firstAfter)
	defer firstTimer.Stop()

	var (
		ticker *time.Ticker
		tick   <-chan time.Time
	)

	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	for {
		select {
		case <-done:
			return
		case <-firstTimer.C:
			l.Printf("[%s] WARNING: %s still blocked after %s (%s)", trace, op, firstAfter, details())

			ticker = time.NewTicker(every)
			tick = ticker.C
		case <-tick:
			l.Printf("[%s] WARNING: %s still blocked (%s)", trace, op, details())
		}
	}
}
