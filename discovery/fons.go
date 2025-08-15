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

package discovery

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/wtsi-hgi/ibackup/db"
)

var newline = []byte{'\n'} //nolint:gochecknoglobals

func readFons(transformer *db.Transformer,
	fons []*db.Discover, filter StateMachine[bool]) ([]string, error) {
	var list []string

	for _, fon := range fons {
		contents, err := readFon(transformer, fon, filter)
		if err != nil {
			return nil, err
		}

		list = append(list, contents...)
	}

	return list, nil
}

func ReadFon(transformer *db.Transformer, d *db.Discover) ([]string, error) {
	return readFon(transformer, d, nil)
}

func readFon(transformer *db.Transformer, d *db.Discover, filter StateMachine[bool]) ([]string, error) {
	decoder := nullDecoder

	switch d.Type { //nolint:exhaustive
	case db.DiscoverFODNBase64, db.DiscoverFOFNBase64:
		decoder = base64Decoder
	case db.DiscoverFODNQuoted, db.DiscoverFOFNQuoted:
		decoder = strconv.Unquote
	}

	splitByte := byte('\n')

	switch d.Type { //nolint:exhaustive
	case db.DiscoverFODNNull, db.DiscoverFOFNNull:
		splitByte = '\x00'
	}

	var dirs bool

	switch d.Type { //nolint:exhaustive
	case db.DiscoverFODN, db.DiscoverFODNBase64, db.DiscoverFODNQuoted:
		dirs = true
	}

	f, err := os.Open(d.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return readLines(transformer, f, decoder, dirs, filter, splitByte)
}

func nullDecoder(str string) (string, error) {
	return str, nil
}

func base64Decoder(str string) (string, error) {
	fn, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}

	return toString(fn), nil
}

func toString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func toBytes(b string) []byte {
	return unsafe.Slice(unsafe.StringData(b), len(b))
}

func readLines(trns *db.Transformer, r io.Reader, decoder func(string) (string, error),
	dirs bool, filter StateMachine[bool], splitByte byte) ([]string, error) {
	var lines []string

	buf := bufio.NewReader(r)

	for {
		line, err := buf.ReadBytes(splitByte)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		var errl error

		lines, errl = parseLine(lines, line, trns, decoder, dirs, filter)
		if errl != nil {
			return nil, err
		} else if errors.Is(err, io.EOF) {
			return lines, nil
		}
	}
}

func parseLine(lines []string, line []byte, trns *db.Transformer, //nolint:gocyclo
	decoder func(string) (string, error), dirs bool, filter StateMachine[bool]) ([]string, error) {
	line = bytes.TrimSuffix(line, newline)

	decoded, errd := decoder(toString(line))
	if errd != nil {
		return nil, errd
	}

	decoded = filepath.Clean(decoded)

	if dirs && !strings.HasSuffix(decoded, "/") {
		decoded += "/"
	}

	if filter != nil {
		v := filter.Match(toBytes(decoded))

		if v != nil && !*v {
			return lines, nil
		}
	}

	if !trns.Match(decoded) {
		return nil, fmt.Errorf("path: %s: %w", decoded, db.ErrInvalidTransformPath)
	}

	return append(lines, decoded), nil
}
