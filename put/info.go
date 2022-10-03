/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package put

import "time"

const objectInfoMtimeKey = "mtime"

type ObjectInfo struct {
	Exists bool
	Meta   map[string]string
}

// ModTime converts our mtime metadata key value string to a time.Time. Returns
// time zero if the key isn't present or convertable.
func (o *ObjectInfo) ModTime() time.Time {
	t := time.Time{}

	s, exists := o.Meta[objectInfoMtimeKey]
	if !exists || s == "" {
		return t
	}

	t.UnmarshalText([]byte(s)) //nolint:errcheck

	return t
}

// timeToMeta converts a time to a string suitable for storing as metadata, in
// a way that ObjectInfo.ModTime() will understand and be able to convert back
// again.
func timeToMeta(t time.Time) string {
	b, err := t.UTC().Truncate(time.Second).MarshalText()
	if err != nil {
		return ""
	}

	return string(b)
}
