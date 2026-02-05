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

package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/wtsi-hgi/ibackup/transformer"
)

type tx struct {
	Description string
	Re, Replace string
}

// Config stores the global config for the commands.
//
// Currently just stores the names transformers.
var Config struct {
	Transformers map[string]tx `json:"transformers"`
}

func newConfig(config string) error {
	f, err := os.Open(config)
	if err != nil {
		return fmt.Errorf("error opening ibackup config: %w", err)
	}

	if err = json.NewDecoder(f).Decode(&Config); err != nil {
		return fmt.Errorf("error parsing ibackup config: %w", err)
	}

	return f.Close()
}

// LoadConfig loads global config from the given path.
func LoadConfig(path string) error {
	if err := newConfig(path); err != nil {
		return err
	}

	for name, tx := range Config.Transformers {
		err := transformer.Register(name, tx.Re, tx.Replace)
		if err != nil {
			return fmt.Errorf("error registering transformer (%s): %w", name, err)
		}
	}

	updateAddDescFlag()
	addRemoteCmdFlags()
	updateEditFlagText()

	return nil
}
