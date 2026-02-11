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
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig(t *testing.T) {
	Convey("ReadConfig and WriteConfig", t, func() {
		dir := t.TempDir()

		writeYAML := func(subdir, content string) string {
			sub := filepath.Join(dir, subdir)
			So(os.MkdirAll(sub, 0750), ShouldBeNil)
			So(os.WriteFile(
				filepath.Join(sub, "config.yml"),
				[]byte(content), 0600, //nolint:gosec
			), ShouldBeNil)

			return sub
		}

		Convey("transformer humgen, freeze true", func() {
			sub := writeYAML("t1",
				"transformer: humgen\nfreeze: true\n")

			cfg, err := ReadConfig(sub)
			So(err, ShouldBeNil)
			So(cfg.Transformer, ShouldEqual, "humgen")
			So(cfg.Freeze, ShouldBeTrue)
			So(cfg.Metadata, ShouldBeNil)
		})

		Convey("transformer humgen with metadata", func() {
			sub := writeYAML("t2",
				"transformer: humgen\nmetadata:\n  colour: red\n  size: large\n")

			cfg, err := ReadConfig(sub)
			So(err, ShouldBeNil)
			So(cfg.Transformer, ShouldEqual, "humgen")
			So(cfg.Freeze, ShouldBeFalse)
			So(cfg.Metadata, ShouldResemble,
				map[string]string{
					"colour": "red",
					"size":   "large",
				})
		})

		Convey("no config.yml returns error", func() {
			sub := filepath.Join(dir, "noconfig")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
		})

		Convey("no transformer key returns error", func() {
			sub := writeYAML("t4", "freeze: true\n")

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
		})

		Convey("empty transformer value returns error", func() {
			sub := writeYAML("t5",
				"transformer: \"\"\nfreeze: true\n")

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
		})

		Convey("freeze omitted defaults to false", func() {
			sub := writeYAML("t6", "transformer: humgen\n")

			cfg, err := ReadConfig(sub)
			So(err, ShouldBeNil)
			So(cfg.Freeze, ShouldBeFalse)
		})

		Convey("metadata omitted defaults to nil", func() {
			sub := writeYAML("t7", "transformer: humgen\n")

			cfg, err := ReadConfig(sub)
			So(err, ShouldBeNil)
			So(cfg.Metadata, ShouldBeNil)
		})

		Convey("metadata key with colon returns error", func() {
			sub := writeYAML("t8",
				"transformer: humgen\nmetadata:\n  \"bad:key\": value\n")

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
		})

		Convey("WriteConfig + ReadConfig round-trip with freeze and metadata", func() {
			sub := filepath.Join(dir, "rt1")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			orig := SubDirConfig{
				Transformer: "gendev",
				Freeze:      true,
				Metadata: map[string]string{
					"project": "alpha",
					"team":    "beta",
				},
			}

			err := WriteConfig(sub, orig)
			So(err, ShouldBeNil)

			got, err := ReadConfig(sub)
			So(err, ShouldBeNil)
			So(got.Transformer, ShouldEqual, orig.Transformer)
			So(got.Freeze, ShouldEqual, orig.Freeze)
			So(got.Metadata, ShouldResemble, orig.Metadata)
		})

		Convey("WriteConfig + ReadConfig round-trip without freeze/metadata", func() {
			sub := filepath.Join(dir, "rt2")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			orig := SubDirConfig{
				Transformer: "humgen",
			}

			err := WriteConfig(sub, orig)
			So(err, ShouldBeNil)

			got, err := ReadConfig(sub)
			So(err, ShouldBeNil)
			So(got.Transformer, ShouldEqual, "humgen")
			So(got.Freeze, ShouldBeFalse)
			So(got.Metadata, ShouldBeNil)
		})

		Convey("WriteConfig with empty Transformer returns error, no file", func() {
			sub := filepath.Join(dir, "empty_tx")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			err := WriteConfig(sub, SubDirConfig{})
			So(err, ShouldNotBeNil)

			_, statErr := os.Stat(filepath.Join(sub, "config.yml"))
			So(os.IsNotExist(statErr), ShouldBeTrue)
		})

		Convey("ReadConfig rejects metadata value containing =", func() {
			sub := filepath.Join(dir, "bad_eq")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			So(os.WriteFile(
				filepath.Join(sub, "config.yml"),
				[]byte("transformer: test\nmetadata:\n  key: val=ue\n"),
				0600,
			), ShouldBeNil)

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
			So(errors.Is(err, ErrMetadataDelimiter), ShouldBeTrue)
		})

		Convey("ReadConfig rejects metadata value containing ;", func() {
			sub := filepath.Join(dir, "bad_semi")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			So(os.WriteFile(
				filepath.Join(sub, "config.yml"),
				[]byte("transformer: test\nmetadata:\n  key: va;lue\n"),
				0600,
			), ShouldBeNil)

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
			So(errors.Is(err, ErrMetadataDelimiter), ShouldBeTrue)
		})

		Convey("ReadConfig rejects metadata key containing =", func() {
			sub := filepath.Join(dir, "bad_key_eq")
			So(os.MkdirAll(sub, 0750), ShouldBeNil)

			So(os.WriteFile(
				filepath.Join(sub, "config.yml"),
				[]byte("transformer: test\nmetadata:\n  k=ey: value\n"),
				0600,
			), ShouldBeNil)

			_, err := ReadConfig(sub)
			So(err, ShouldNotBeNil)
			So(errors.Is(err, ErrMetadataDelimiter), ShouldBeTrue)
		})
	})

	Convey("UserMetaString", t, func() {
		Convey("with metadata returns sorted key=value pairs", func() {
			cfg := SubDirConfig{
				Metadata: map[string]string{
					"colour": "red",
					"size":   "large",
				},
			}
			So(cfg.UserMetaString(), ShouldEqual,
				"colour=red;size=large")
		})

		Convey("with nil metadata returns empty string", func() {
			cfg := SubDirConfig{}
			So(cfg.UserMetaString(), ShouldEqual, "")
		})
	})
}
