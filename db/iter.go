/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package db

import "iter"

type scanner interface {
	Scan(dest ...any) error
}

type IterErr[T any] struct {
	Iter  iter.Seq[T]
	Error error
}

func noSeq[T any](_ func(T) bool) {}

func iterRows[T any](d *DBRO, scanner func(scanner) (T, error), query string, args ...any) *IterErr[T] {
	var ie IterErr[T]

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return iterErr[T](err)
	}

	ie.Iter = func(yield func(T) bool) {
		defer rows.Close()

		for rows.Next() {
			v, err := scanner(rows)
			if err != nil {
				ie.Error = err

				return
			}

			if !yield(v) {
				break
			}
		}

		ie.Error = rows.Err()
	}

	return &ie
}

func iterErr[T any](err error) *IterErr[T] {
	return &IterErr[T]{
		Iter:  noSeq[T],
		Error: err,
	}
}

func (i *IterErr[T]) ForEach(fn func(T) error) error {
	for item := range i.Iter {
		if err := fn(item); err != nil {
			return err
		}
	}

	return i.Error
}
