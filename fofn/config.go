/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package fofn

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	configFilename = "config.yml"
	configFileMode = 0640
)

// ErrEmptyTransformer is returned when the transformer
// field is missing or empty.
var ErrEmptyTransformer = errors.New("config missing or empty transformer")

// ErrMetadataKeyColon is returned when a metadata key contains a colon.
var ErrMetadataKeyColon = errors.New("metadata key contains colon")

// ErrMetadataDelimiter is returned when a metadata key or value contains
// a semicolon or equals sign, which would break the serialized meta string.
var ErrMetadataDelimiter = errors.New("metadata key or value contains '=' or ';'")

// SubDirConfig holds configuration for a watched subdirectory.
type SubDirConfig struct {
	Transformer string            `yaml:"transformer"`
	Freeze      bool              `yaml:"freeze,omitempty"`
	Metadata    map[string]string `yaml:"metadata,omitempty"`
}

// ReadConfig reads config.yml from dir and returns the parsed SubDirConfig.
// Returns an error if the file is missing, transformer is empty, or metadata
// keys contain colons.
func ReadConfig(dir string) (SubDirConfig, error) {
	data, err := os.ReadFile(filepath.Join(dir, configFilename))
	if err != nil {
		return SubDirConfig{}, fmt.Errorf(
			"read config: %w", err,
		)
	}

	var cfg SubDirConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return SubDirConfig{}, fmt.Errorf(
			"parse config: %w", err,
		)
	}

	return cfg, validateConfig(cfg)
}

// UserMetaString returns a semicolon-separated string of
// sorted key=value pairs from the metadata map.
func (c SubDirConfig) UserMetaString() string {
	if len(c.Metadata) == 0 {
		return ""
	}

	keys := make([]string, 0, len(c.Metadata))
	for k := range c.Metadata {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, k+"="+c.Metadata[k])
	}

	return strings.Join(pairs, ";")
}

// WriteConfig writes cfg as config.yml in dir. Returns an
// error if Transformer is empty.
func WriteConfig(dir string, cfg SubDirConfig) error {
	if cfg.Transformer == "" {
		return ErrEmptyTransformer
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return os.WriteFile(filepath.Join(dir, configFilename), data, configFileMode)
}

func validateConfig(cfg SubDirConfig) error {
	if cfg.Transformer == "" {
		return ErrEmptyTransformer
	}

	for key, val := range cfg.Metadata {
		if strings.Contains(key, ":") {
			return fmt.Errorf("%w: %q", ErrMetadataKeyColon, key)
		}

		if strings.ContainsAny(key, "=;") {
			return fmt.Errorf("%w: key %q", ErrMetadataDelimiter, key)
		}

		if strings.ContainsAny(val, "=;") {
			return fmt.Errorf("%w: value %q", ErrMetadataDelimiter, val)
		}
	}

	return nil
}
