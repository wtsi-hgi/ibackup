/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
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

package transfer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
)

const configFileEqualSplitParts = 2

var (
	defRegMu        sync.RWMutex         //nolint:gochecknoglobals
	defaultRegistry *TransformerRegistry //nolint:gochecknoglobals
)

// TransformerRegistry manages a collection of named RegexTransformer.
type TransformerRegistry struct {
	transformers map[string]*RegexTransformer
}

// NewTransformerRegistry creates a new empty transformer registry.
func NewTransformerRegistry() *TransformerRegistry {
	return &TransformerRegistry{
		transformers: make(map[string]*RegexTransformer),
	}
}

// Register adds a new RegexTransformer to the registry.
func (tr *TransformerRegistry) Register(name, description, matchPattern, replacePattern string) error {
	match, err := regexp.Compile(matchPattern)
	if err != nil {
		return fmt.Errorf("invalid match pattern: %w", err)
	}

	tr.transformers[name] = &RegexTransformer{
		Name:        name,
		Description: description,
		Match:       match,
		Replace:     replacePattern,
	}

	return nil
}

// Get retrieves a transformer by name. The bool tells you if a transformer with
// that name was found.
func (tr *TransformerRegistry) Get(name string) (TransformerInfo, bool) {
	t, exists := tr.transformers[name]
	if !exists {
		var nilTransformer TransformerInfo

		return nilTransformer, false
	}

	return t.ToInfo(), true
}

// GetAll returns all registered transformers with their metadata.
func (tr *TransformerRegistry) GetAll() []TransformerInfo {
	result := make([]TransformerInfo, 0, len(tr.transformers))

	for _, t := range tr.transformers {
		result = append(result, t.ToInfo())
	}

	return result
}

// ParseConfig creates a registry from a config file. Format:
//
// [transformer]
// name = transformerName
// description = description text
// match = regex pattern
// replace = replacement pattern
//
// [transformer]
// ...
func ParseConfig(config io.Reader) (*TransformerRegistry, error) {
	registry := NewTransformerRegistry()
	scanner := bufio.NewScanner(config)

	if err := processConfigLines(scanner, registry); err != nil {
		return nil, err
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return registry, nil
}

type transformerData struct {
	name, description, match, replace string
}

func processConfigLines(scanner *bufio.Scanner, registry *TransformerRegistry) error {
	transformer := &transformerData{}
	currentSection := ""

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if shouldSkipLine(line) {
			continue
		}

		if isSectionHeader(line) {
			if err := handleSectionTransition(registry, currentSection, transformer); err != nil {
				return err
			}

			currentSection = extractSectionName(line)

			continue
		}

		processLineInSection(line, currentSection, transformer)
	}

	return finalizeConfiguration(registry, currentSection, transformer)
}

func shouldSkipLine(line string) bool {
	return line == "" || strings.HasPrefix(line, "#")
}

func isSectionHeader(line string) bool {
	return strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]")
}

func handleSectionTransition(registry *TransformerRegistry, currentSection string, transformer *transformerData) error {
	if shouldRegisterTransformer(currentSection, transformer) {
		return registerAndReset(registry, transformer)
	}

	return nil
}

func shouldRegisterTransformer(section string, t *transformerData) bool {
	return section == "transformer" && t.name != "" && t.match != "" && t.replace != ""
}

func registerAndReset(registry *TransformerRegistry, t *transformerData) error {
	err := registry.Register(t.name, t.description, t.match, t.replace)
	if err != nil {
		return err
	}

	t.name, t.description, t.match, t.replace = "", "", "", ""

	return nil
}

func extractSectionName(line string) string {
	return strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
}

func processLineInSection(line, currentSection string, transformer *transformerData) {
	if currentSection == "transformer" {
		processTransformerProperty(line, transformer)
	}
}

func processTransformerProperty(line string, t *transformerData) {
	parts := strings.SplitN(line, "=", -1)
	if len(parts) != configFileEqualSplitParts {
		return
	}

	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])

	switch key {
	case "name":
		t.name = value
	case "description":
		t.description = value
	case "match":
		t.match = value
	case "replace":
		t.replace = value
	}
}

func finalizeConfiguration(registry *TransformerRegistry, currentSection string, transformer *transformerData) error {
	if shouldRegisterTransformer(currentSection, transformer) {
		return registry.Register(transformer.name, transformer.description, transformer.match, transformer.replace)
	}

	return nil
}

// SetDefaultRegistry sets the default tranformer registry used globally.
func SetDefaultRegistry(r *TransformerRegistry) {
	defRegMu.Lock()
	defer defRegMu.Unlock()

	defaultRegistry = r
}

// DefaultRegistry returns the default transformer registry used globally.
func DefaultRegistry() *TransformerRegistry {
	defRegMu.RLock()
	defer defRegMu.RUnlock()

	return defaultRegistry
}

// LookupTransformer looks up a transformer by name in the default registry.
func LookupTransformer(name string) (PathTransformer, bool) {
	defRegMu.RLock()
	r := defaultRegistry
	defRegMu.RUnlock()

	if r == nil {
		return nil, false
	}

	info, ok := r.Get(name)
	if !ok {
		return nil, false
	}

	return info.Transformer, true
}

// SetDefaultRegistryFromFile sets the default transformer registry used
// globally from a file using ParseConfig().
func SetDefaultRegistryFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	defer f.Close()

	reg, err := ParseConfig(f)
	if err != nil {
		return err
	}

	SetDefaultRegistry(reg)

	return nil
}
