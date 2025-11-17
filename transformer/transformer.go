/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package transformer

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

const prefixPrefix = "prefix="

var (
	ErrInvalidReplace       = errors.New("invalid replace string")
	ErrInvalidTransformPath = errors.New("invalid transform path")
	ErrInvalidTransformer   = errors.New("invalid transformer")
)

type transformer struct {
	re      *regexp.Regexp
	replace string
}

type tx interface {
	Transform(path string) (string, error)
}

var (
	mu           sync.RWMutex      //nolint:gochecknoglobals
	transformers = map[string]tx{} //nolint:gochecknoglobals
)

func newTransformer(name, match, replace string) (*transformer, error) {
	re, err := regexp.Compile(match)
	if err != nil {
		return nil, err
	}

	if re.NumSubexp() < strings.Count(replace, "$") {
		return nil, ErrInvalidReplace
	}

	tx := &transformer{
		re:      re,
		replace: replace,
	}

	mu.Lock()

	transformers[name] = tx

	mu.Unlock()

	return tx, nil
}

func (t *transformer) Match(path string) bool {
	return t.re.MatchString(path)
}

func (t *transformer) Transform(path string) (string, error) {
	if !t.Match(path) {
		return "", fmt.Errorf("%s: %w", path, ErrInvalidTransformPath)
	}

	return filepath.Clean(t.re.ReplaceAllString(path, t.replace)), nil
}

// Transform transforms the given local path to a remote path using the given
// named or prefix transformer.
func Transform(transformer string, path string) (string, error) {
	mu.RLock()

	txa, ok := transformers[transformer]

	mu.RUnlock()

	if !ok { //nolint:nestif
		if strings.HasPrefix(transformer, prefixPrefix) {
			var err error

			txa, err = compilePrefixTransformer(transformer)
			if err != nil {
				return "", nil //nolint:nilerr
			}
		} else {
			return "", ErrInvalidTransformer
		}
	}

	return txa.Transform(path)
}

// ValidateTransformer confirms the existence of a named transformer, or whether
// a given prefix transform is of the correct format.
func ValidateTransformer(transformer string) error {
	mu.RLock()

	_, ok := transformers[transformer]

	mu.RUnlock()

	if ok {
		return nil
	}

	_, err := compilePrefixTransformer(transformer)

	return err
}

// PathTransformer is a function that given a local path, returns the
// corresponding remote path.
type PathTransformer func(local string) (remote string, err error)

// MakePathTransformer returns a function that used the given named or prefix
// transformer to transform local paths to remote paths.
func MakePathTransformer(tx string) (PathTransformer, error) {
	if err := ValidateTransformer(tx); err != nil {
		return nil, err
	}

	return func(path string) (string, error) {
		return Transform(tx, path)
	}, nil
}

// Register creates a named transformer from the given regex an replacement
// strings.
func Register(name string, re, replace string) error {
	_, err := newTransformer(name, re, replace)

	return err
}

type prefixTransformer struct {
	local, remote string
}

func (p *prefixTransformer) Transform(path string) (string, error) {
	return filepath.Join(p.remote, strings.TrimPrefix(path, p.local)), nil
}

func compilePrefixTransformer(trans string) (tx, error) { //nolint:ireturn
	if !strings.HasPrefix(trans, prefixPrefix) {
		return nil, ErrInvalidTransformer
	}

	local, remote, _ := strings.Cut(strings.TrimPrefix(trans, prefixPrefix), ":")

	local = filepath.Clean(local) + "/"
	remote = filepath.Clean(remote) + "/"

	tx := &prefixTransformer{
		local:  local,
		remote: remote,
	}

	mu.Lock()

	transformers[trans] = tx

	mu.Unlock()

	return tx, nil
}
