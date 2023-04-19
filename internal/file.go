/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package internal

import (
	"os"
	"time"
)

const (
	fileCheckFrequency = 10 * time.Millisecond
	fileCheckTimeout   = 5 * time.Second
)

// WaitForFile waits for up to 5 seconds for the given path to exist, and
// returns false if it doesn't.
func WaitForFile(path string) bool {
	ticker := time.NewTicker(fileCheckFrequency)
	defer ticker.Stop()

	timeout := time.NewTimer(fileCheckTimeout)

	for {
		select {
		case <-timeout.C:
			return false
		case <-ticker.C:
			_, err := os.Stat(path)
			if err == nil {
				return true
			}
		}
	}
}

// WaitForFileChange waits for up to 5 seconds for the given path to change, and
// returns false if it doesn't.
func WaitForFileChange(path string, lastMod time.Time) bool {
	ticker := time.NewTicker(fileCheckFrequency)
	defer ticker.Stop()

	timeout := time.NewTimer(fileCheckTimeout)

	for {
		select {
		case <-timeout.C:
			return false
		case <-ticker.C:
			stat, err := os.Stat(path)
			if err == nil && stat.ModTime().After(lastMod) {
				return true
			}
		}
	}
}
