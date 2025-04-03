/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * Author: Iaroslav Popov <ip13@sanger.ac.uk>
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

package errs

import (
	"errors"
	"fmt"
)

type PathError struct {
	Msg  string
	Path string
}

func (e PathError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s [%s]", e.Msg, e.Path)
	}

	return e.Msg
}

func (e PathError) Is(err error) bool {
	var putErr *PathError
	if errors.As(err, &putErr) {
		return putErr.Msg == e.Msg
	}

	return false
}

type DirRemovalError struct {
	PathError
}

func NewDirError(msg, path string) DirRemovalError {
	return DirRemovalError{
		PathError{
			Msg:  "dir removal error: " + msg,
			Path: path,
		},
	}
}

type DirNotEmptyError struct {
	PathError
}

func NewDirNotEmptyError(path string) DirNotEmptyError {
	return DirNotEmptyError{
		PathError{
			Msg:  "directory not empty",
			Path: path,
		},
	}
}
