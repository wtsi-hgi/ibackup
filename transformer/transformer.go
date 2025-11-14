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
	id      int64
	name    string
	re      *regexp.Regexp
	replace string
}

type tx interface {
	Transform(path string) (string, error)
}

var (
	mu           sync.RWMutex
	transformers = map[string]tx{}
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
	tx, ok := transformers[transformer]
	mu.RUnlock()

	if !ok {
		if strings.HasPrefix(transformer, prefixPrefix) {
			var err error
			tx, err = compilePrefixTransformer(transformer)
			if err != nil {
				return "", nil
			}
		} else {
			return "", ErrInvalidTransformer
		}
	}

	return tx.Transform(path)
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

func compilePrefixTransformer(trans string) (tx, error) {
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
