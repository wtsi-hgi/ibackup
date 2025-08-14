/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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
	"regexp"
	"strings"
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

	var currentSection, name, description, match, replace string

	scanner := bufio.NewScanner(config)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			if currentSection == "transformer" && name != "" && match != "" && replace != "" {
				err := registry.Register(name, description, match, replace)
				if err != nil {
					return nil, err
				}

				name, description, match, replace = "", "", "", ""
			}

			currentSection = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")

			continue
		}

		if currentSection == "transformer" {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) != 2 {
				continue
			}

			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			switch key {
			case "name":
				name = value
			case "description":
				description = value
			case "match":
				match = value
			case "replace":
				replace = value
			}
		}
	}

	if currentSection == "transformer" && name != "" && match != "" && replace != "" {
		err := registry.Register(name, description, match, replace)
		if err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return registry, nil
}
