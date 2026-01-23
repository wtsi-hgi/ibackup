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

package testutil

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type transformerConfig struct {
	Description string
	Re          string
	Replace     string
}

// WriteDefaultConfig writes a default config and sets IBACKUP_CONFIG.
func WriteDefaultConfig(tb testing.TB) string {
	tb.Helper()

	conf := filepath.Join(tb.TempDir(), "config.json")

	writeConfigFile(tb, conf, defaultTransformers())
	setTestEnv(tb, "IBACKUP_CONFIG", conf)

	return conf
}

func defaultTransformers() map[string]transformerConfig {
	genRe := `^/lustre/(scratch[^/]+)(/[^/]*)+?/([pP]rojects|teams|users)(_v2)?/([^/]+)/`

	return map[string]transformerConfig{
		"humgen": {Re: genRe, Replace: "/humgen/$3/$5/$1$4/"},
		"gengen": {Re: genRe, Replace: "/humgen/gengen/$3/$5/$1$4/"},
		"otar":   {Re: genRe, Replace: "/humgen/open-targets/$3/$5/$1$4/"},
	}
}

func writeConfigFile(tb testing.TB, path string, transformers map[string]transformerConfig) {
	tb.Helper()

	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("failed to create config: %v", err)
	}

	if err := json.NewEncoder(f).Encode(struct {
		Transformers map[string]transformerConfig `json:"transformers"`
	}{
		Transformers: transformers,
	}); err != nil {
		_ = f.Close()

		tb.Fatalf("failed to write config: %v", err)
	}

	if err := f.Close(); err != nil {
		tb.Fatalf("failed to close config: %v", err)
	}
}

func setTestEnv(tb testing.TB, key, value string) {
	tb.Helper()

	if envSetter, ok := tb.(interface{ Setenv(key, value string) }); ok {
		envSetter.Setenv(key, value)

		return
	}

	_ = os.Setenv(key, value)
}
