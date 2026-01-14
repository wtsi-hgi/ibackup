package server

import (
	"runtime"
	"time"
)

const (
	minHungDebugTick            = 30 * time.Second
	maxHungDebugTick            = 5 * time.Minute
	hungDebugIntervalDivisor    = 2
	initialGoroutineDumpBufSize = 1 << 20
	maxGoroutineDumpBufSize     = 16 << 20
)

func (s *Server) startHungDebug() {
	if s.hungDebugTimeout <= 0 || s.hungDebugStopCh != nil {
		return
	}

	s.hungDebugStopCh = make(chan struct{})

	// Consider "no status updates yet" as "just now".
	s.hungDebugLastStatus.Store(time.Now().UnixNano())

	s.hungDebugRunLoop(hungDebugTickInterval(s.hungDebugTimeout), s.hungDebugStopCh)
}

func hungDebugTickInterval(timeout time.Duration) time.Duration {
	// We only check periodically and only emit logs if something looks stuck.
	interval := timeout / hungDebugIntervalDivisor
	if interval < minHungDebugTick {
		return minHungDebugTick
	}

	if interval > maxHungDebugTick {
		return maxHungDebugTick
	}

	return interval
}

func (s *Server) hungDebugRunLoop(interval time.Duration, stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.hungDebugCheck()
			case <-stopCh:
				return
			}
		}
	}()
}

func (s *Server) stopHungDebug() {
	if s.hungDebugStopCh == nil {
		return
	}

	close(s.hungDebugStopCh)
	s.hungDebugStopCh = nil
}

func (s *Server) hungDebugNoteStatusUpdate(rid string) {
	if s.hungDebugTimeout <= 0 {
		return
	}

	s.hungDebugLastRID.Store(rid)
	s.hungDebugLastStatus.Store(time.Now().UnixNano())
}

func (s *Server) hungDebugCheck() {
	if s.hungDebugTimeout <= 0 {
		return
	}

	numUploading, numStuck := s.hungDebugCounts()
	if numUploading == 0 && numStuck == 0 {
		return
	}

	now := time.Now()
	if s.hungDebugRateLimited(now) {
		return
	}

	if s.hungDebugCheckNoStatusUpdates(now, numUploading, numStuck) {
		return
	}

	s.hungDebugCheckStuckUploads(now, numUploading, numStuck)
}

func (s *Server) hungDebugCounts() (int, int) {
	if s.uploadTracker == nil {
		return 0, 0
	}

	return s.uploadTracker.numUploading(), s.uploadTracker.numStuck()
}

func (s *Server) hungDebugRateLimited(now time.Time) bool {
	// Rate limit: once per timeout.
	lastLog := time.Unix(0, s.hungDebugLastLog.Load())

	return lastLog.After(now.Add(-s.hungDebugTimeout))
}

func (s *Server) hungDebugLastRIDString() string {
	rid, ok := s.hungDebugLastRID.Load().(string)
	if !ok {
		return ""
	}

	return rid
}

func (s *Server) hungDebugCheckNoStatusUpdates(now time.Time, numUploading, numStuck int) bool {
	if numUploading == 0 {
		return false
	}

	lastStatus := time.Unix(0, s.hungDebugLastStatus.Load())

	statusAge := now.Sub(lastStatus)
	if statusAge <= s.hungDebugTimeout {
		return false
	}

	s.Logger.Printf(
		"hung? no file status updates for %s while uploads in progress uploading=%d stuck=%d last_rid=%s",
		statusAge.Truncate(time.Second),
		numUploading,
		numStuck,
		s.hungDebugLastRIDString(),
	)
	s.Logger.Printf("hung? goroutine dump follows\n%s", dumpAllGoroutines())

	s.hungDebugLastLog.Store(now.UnixNano())

	return true
}

func (s *Server) hungDebugCheckStuckUploads(now time.Time, numUploading, numStuck int) {
	// Case 2: stuck uploads present (client marked). If the oldest stuck upload
	// has exceeded the timeout, dump more detail.
	if numStuck == 0 || s.uploadTracker == nil {
		return
	}

	oldestDur, oldestRID, oldestDesc := s.hungDebugOldestStuck(now)
	if oldestDur <= s.hungDebugTimeout {
		return
	}

	s.Logger.Printf(
		"hung? stuck uploads present stuck=%d uploading=%d oldest_stuck=%s oldest_rid=%s oldest=%s",
		numStuck,
		numUploading,
		oldestDur.Truncate(time.Second),
		oldestRID,
		oldestDesc,
	)
	s.Logger.Printf("hung? goroutine dump follows\n%s", dumpAllGoroutines())

	s.hungDebugLastLog.Store(now.UnixNano())
}

func (s *Server) hungDebugOldestStuck(now time.Time) (time.Duration, string, string) {
	stuck := s.uploadTracker.currentlyStuck()

	var (
		oldestDur  time.Duration
		oldestRID  string
		oldestDesc string
	)

	for _, r := range stuck {
		if r == nil || r.Stuck == nil {
			continue
		}

		d := now.Sub(r.Stuck.UploadStarted)
		if d > oldestDur {
			oldestDur = d
			oldestRID = r.ID()
			oldestDesc = r.Stuck.String()
		}
	}

	return oldestDur, oldestRID, oldestDesc
}

func dumpAllGoroutines() string {
	size := initialGoroutineDumpBufSize
	for {
		buf := make([]byte, size)

		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}

		// Cap growth so we don't explode memory in pathological cases.
		if size >= maxGoroutineDumpBufSize {
			return string(buf[:n])
		}

		size *= 2
	}
}
