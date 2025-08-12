package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	ErrInvalidReplace       = errors.New("invalid replace string")
	ErrInvalidTransformPath = errors.New("invalid transform path")
	ErrInvalidTransformer   = errors.New("invalid transformer")
)

type Transformer struct {
	name    string
	re      *regexp.Regexp
	replace string
}

func NewTransformer(name, match, replace string) (*Transformer, error) {
	re, err := regexp.Compile(match)
	if err != nil {
		return nil, err
	}

	if re.NumSubexp() < strings.Count(replace, "$") {
		return nil, ErrInvalidReplace
	}

	return &Transformer{
		name:    name,
		re:      re,
		replace: replace,
	}, nil
}

func (t *Transformer) isValid() bool {
	return t != nil && t.re != nil
}

func (t *Transformer) Name() string {
	return t.name
}

func (t *Transformer) Match(path string) bool {
	return t.re.MatchString(path)
}

func (t *Transformer) Transform(path string) (string, error) {
	if !t.Match(path) {
		return "", fmt.Errorf("path: %s: %w", path, ErrInvalidTransformPath)
	}

	return filepath.Clean(t.re.ReplaceAllString(path, t.replace)), nil
}
