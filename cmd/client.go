/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/wtsi-hgi/ibackup/db"
)

var dbDriver = "mysql"

type transformer struct {
	Re      string `json:"re"`
	Replace string `json:"replace"`
}

type transformers map[string]transformer

func (t transformers) Compile(name string) (*db.Transformer, error) {
	trans, ok := t[name]
	if !ok {
		return compilePrefixTransformer(name)
	}

	return db.NewTransformer(name, trans.Re, trans.Replace)
}

func compilePrefixTransformer(trans string) (*db.Transformer, error) {
	const prefixPrefix = "prefix="

	if !strings.HasPrefix(trans, prefixPrefix) {
		return nil, db.ErrInvalidTransformer
	}

	parts := strings.SplitN(strings.TrimPrefix(trans, prefixPrefix), ":", 2)
	if len(parts) != 2 {
		return nil, db.ErrInvalidTransformer
	}

	parts[0] = filepath.Clean(parts[0]) + "/"
	parts[1] = filepath.Clean(parts[1]) + "/"

	return db.NewTransformer(trans, "^"+regexp.QuoteMeta(parts[0]), strings.ReplaceAll(parts[1], "$", "$$"))
}

type Config struct {
	DBURI string `json:"dburi"`

	// SlackToken is the token you get from the OAuth&Permissions tab in your slack
	// application's features.
	SlackToken string `json:"slackToken"`

	// SlackChannel is the channel ID you get after pressing the 'Get channel
	// details' button (channel title) in any channel, the SlackChannel ID is at the
	// bottom of the pop-up box.
	SlackChannel string `json:"slackChannel"`

	WRConfig string `json:"wrConfig"`

	Transformers transformers `json:"transformers"`
}

func newConfig(config string) (*Config, error) {
	f, err := os.Open(config)
	if err != nil {
		return nil, fmt.Errorf("error opening ibackup config: %w", err)
	}

	var conf Config

	if err = json.NewDecoder(f).Decode(&conf); err != nil {
		return nil, fmt.Errorf("error parsing ibackup config: %w", err)
	}

	return &conf, nil
}

func currentUsername() string {
	user, err := user.Current()
	if err != nil {
		dief("couldn't get user: %s", err)
	}

	return user.Username
}

func isExeOwner() bool {
	exe, err := os.Executable()
	if err != nil {
		return false
	}

	stat, err := os.Stat(exe)
	if err != nil {
		return false
	}

	statt, ok := stat.Sys().(*syscall.Stat_t)

	return ok && int(statt.Uid) == syscall.Getuid()
}

func dropSuid() error {
	return syscall.Setuid(os.Getuid())
}
