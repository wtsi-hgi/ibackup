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
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	levelDebug = "debug"
	levelInfo  = "info"
	levelWarn  = "warn"
)

var (
	errTestX    = errors.New("x")
	errTestBoom = errors.New("boom")
)

type captured struct {
	level string
	msg   string
	ctx   []interface{}
}

func TestStartOperation(t *testing.T) {
	Convey("StartOperation", t, func() {
		Convey("With nil logger, finish is a no-op", func() {
			finish := StartOperation(nil, "op", 1*time.Millisecond, "k", "v")

			So(func() { finish(errTestX) }, ShouldNotPanic)
		})

		Convey("It logs starting, still-running, and finished", func() {
			l := &fakeStructuredLogger{}

			finish := StartOperation(l, "op", 20*time.Millisecond, "k", "v")
			So(waitUntil(200*time.Millisecond, func() bool {
				calls := l.snapshot()
				for _, c := range calls {
					if c.level == levelWarn && c.msg == "op still running" {
						return true
					}
				}

				return false
			}), ShouldBeTrue)

			finish(errTestBoom)

			So(waitUntil(200*time.Millisecond, func() bool {
				calls := l.snapshot()
				starting := false
				finished := false

				for _, c := range calls {
					if c.level == levelInfo && c.msg == "op starting" {
						starting = true
					}

					if c.level == levelInfo && c.msg == "op finished" {
						finished = true
					}
				}

				return starting && finished
			}), ShouldBeTrue)

			var finished captured

			calls := l.snapshot()

			for i := len(calls) - 1; i >= 0; i-- {
				if calls[i].level == levelInfo && calls[i].msg == "op finished" {
					finished = calls[i]

					break
				}
			}

			So(finished.ctx, ShouldContain, "k")
			So(finished.ctx, ShouldContain, "v")
			So(finished.ctx, ShouldContain, "took")
			So(finished.ctx, ShouldContain, "err")
			So(finished.ctx, ShouldContain, errTestBoom)
		})
	})
}

func waitUntil(deadline time.Duration, cond func() bool) bool {
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return true
		}

		time.Sleep(5 * time.Millisecond)
	}

	return cond()
}

type fakeStructuredLogger struct {
	mu    sync.Mutex
	calls []captured
}

func (f *fakeStructuredLogger) Debug(msg string, ctx ...interface{}) { f.add(levelDebug, msg, ctx...) }

func (f *fakeStructuredLogger) Info(msg string, ctx ...interface{}) { f.add(levelInfo, msg, ctx...) }

func (f *fakeStructuredLogger) Warn(msg string, ctx ...interface{}) { f.add(levelWarn, msg, ctx...) }

func (f *fakeStructuredLogger) add(level, msg string, ctx ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, captured{level: level, msg: msg, ctx: append([]interface{}(nil), ctx...)})
}

func (f *fakeStructuredLogger) snapshot() []captured {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]captured, len(f.calls))
	copy(out, f.calls)

	return out
}

type fakePrintfLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (f *fakePrintfLogger) Printf(format string, a ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.msgs = append(f.msgs, fmt.Sprintf(format, a...))
}

func (f *fakePrintfLogger) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, len(f.msgs))
	copy(out, f.msgs)

	return out
}

func TestLogIfBlocked(t *testing.T) {
	Convey("LogIfBlocked", t, func() {
		l := &fakePrintfLogger{}
		done := make(chan struct{})

		go LogIfBlocked(done, l, "trace", "op", 20*time.Millisecond, 30*time.Millisecond, func() string { return "details" })

		So(waitUntil(200*time.Millisecond, func() bool {
			msgs := l.snapshot()
			count := 0

			for _, m := range msgs {
				if strings.Contains(m, "WARNING:") && strings.Contains(m, "still blocked") {
					count++
				}
			}

			return count >= 1
		}), ShouldBeTrue)

		close(done)

		// Give the goroutine a moment to exit cleanly.
		time.Sleep(10 * time.Millisecond)
	})
}
