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
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/ibackup/internal"
)

func TestRegistry(t *testing.T) {
	Convey("You can create and use a transformer registry", t, func() {
		registry := NewTransformerRegistry()

		err := registry.Register("btest", "Test transformer", `/path/to/(.+)`, "/new/path/$1")
		So(err, ShouldBeNil)

		err = registry.Register("atest2", "Test transformer 2", `/path/to/(.+)`, "/new/path/$1")
		So(err, ShouldBeNil)

		info, exists := registry.Get("atest2")
		So(exists, ShouldBeTrue)

		remote, err := info.Transformer("/path/to/file.txt")
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/new/path/file.txt")

		_, exists = registry.Get("nonexistent")
		So(exists, ShouldBeFalse)

		all := registry.GetAll()
		So(len(all), ShouldEqual, 2)
		So(all[0].Name, ShouldEqual, "atest2")
		So(all[0].Description, ShouldEqual, "Test transformer 2")
		So(all[1].Name, ShouldEqual, "btest")
		So(all[1].Description, ShouldEqual, "Test transformer")
	})

	Convey("RegexTransformer transforms paths correctly", t, func() {
		rt := &RegexTransformer{
			Name:        "test",
			Description: "Test transformer",
			Match:       regexp.MustCompile(`/path/to/(.+)`),
			Replace:     "/new/path/$1",
		}

		transformer := rt.PathTransformer()

		result, err := transformer("/path/to/file.txt")
		So(err, ShouldBeNil)
		So(result, ShouldEqual, "/new/path/file.txt")
	})

	Convey("You can parse a config to create a registry", t, func() {
		registry, err := ParseConfig(strings.NewReader(internal.TransformerConfigString))
		So(err, ShouldBeNil)

		humgen, exists := registry.Get("humgen")
		So(exists, ShouldBeTrue)

		locals := []string{
			"/lustre/scratch118/humgen/projects/ddd/file.txt",
			"/lustre/scratch118/humgen/hgi/projects/ibdx10/file.txt",
			"/lustre/scratch118/humgen/hgi/users/hp3/file.txt",
			"/lustre/scratch119/realdata/mdt3/projects/interval_rna/file.txt",
			"/lustre/scratch119/realdata/mdt3/teams/parts/ap32/file.txt",
			"/lustre/scratch123/hgi/mdt2/projects/chromo_ndd/file.txt",
			"/lustre/scratch123/hgi/mdt1/teams/martin/dm22/file.txt",
			"/lustre/scratch123/hgi/mdt1/teams/martin/dm22/sub/folder/file.txt",
			"/lustre/scratch125/humgen/projects_v2/ddd/file.txt",
			"/lustre/scratch127/hgi/mdt1/teams_v2/martin/dm22/file.txt",
			"/lustre/scratch127/hgi/mdt1/teams_v2/martin/dm22/sub/folder/file.txt",
		}

		expected := []string{
			"/humgen/projects/ddd/scratch118/file.txt",
			"/humgen/projects/ibdx10/scratch118/file.txt",
			"/humgen/users/hp3/scratch118/file.txt",
			"/humgen/projects/interval_rna/scratch119/file.txt",
			"/humgen/teams/parts/scratch119/ap32/file.txt",
			"/humgen/projects/chromo_ndd/scratch123/file.txt",
			"/humgen/teams/martin/scratch123/dm22/file.txt",
			"/humgen/teams/martin/scratch123/dm22/sub/folder/file.txt",
			"/humgen/projects/ddd/scratch125_v2/file.txt",
			"/humgen/teams/martin/scratch127_v2/dm22/file.txt",
			"/humgen/teams/martin/scratch127_v2/dm22/sub/folder/file.txt",
		}

		for i, local := range locals {
			result, err := humgen.Transformer(local)
			So(err, ShouldBeNil)
			So(result, ShouldStartWith, expected[i])
		}

		gengen, exists := registry.Get("gengen")
		So(exists, ShouldBeTrue)

		locals = []string{
			"/lustre/scratch126/gengen/teams/lehner/file.txt",
			"/lustre/scratch126/gengen/projects/alpha-allostery-global/file.txt",
			"/lustre/scratch126/gengen/teams/parts/sequencing/file.txt",
			"/lustre/scratch125/gengen/projects_v2/ddd/file.txt",
			"/lustre/scratch127/gengen/teams_v2/parts/sequencing/file.txt",
		}

		expected = []string{
			"/humgen/gengen/teams/lehner/scratch126/file.txt",
			"/humgen/gengen/projects/alpha-allostery-global/scratch126/file.txt",
			"/humgen/gengen/teams/parts/scratch126/sequencing/file.txt",
			"/humgen/gengen/projects/ddd/scratch125_v2/file.txt",
			"/humgen/gengen/teams/parts/scratch127_v2/sequencing/file.txt",
		}

		for i, local := range locals {
			result, err := gengen.Transformer(local)
			So(err, ShouldBeNil)
			So(result, ShouldStartWith, expected[i])
		}
	})
}

func TestDefaultRegistry(t *testing.T) {
	Convey("DefaultRegistry and LookupTransformer work under concurrent access", t, func() {
		regA := NewTransformerRegistry()
		So(regA.Register("testA", "A", `/path/to/(.+)`, "/new/path/$1"), ShouldBeNil)

		regB := NewTransformerRegistry()
		So(regB.Register("testB", "B", `/path/to/(.+)`, "/alt/path/$1"), ShouldBeNil)

		SetDefaultRegistry(regA)

		var wg sync.WaitGroup

		errs := make(chan struct{}, 10000)
		readers, writers := 50, 10

		for range readers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range 500 {
					if r := DefaultRegistry(); r != nil {
						if info, ok := r.Get("testA"); ok {
							out, err := info.Transformer("/path/to/file.txt")
							if err != nil || !strings.HasPrefix(out, "/new/path/") {
								errs <- struct{}{}
							}
						} else if info, ok := r.Get("testB"); ok {
							out, err := info.Transformer("/path/to/file.txt")
							if err != nil || !strings.HasPrefix(out, "/alt/path/") {
								errs <- struct{}{}
							}
						} else {
							errs <- struct{}{}
						}
					}

					if tr, ok := LookupTransformer("testA"); ok {
						out, err := tr("/path/to/file.txt")
						if err != nil || !strings.HasPrefix(out, "/new/path/") {
							errs <- struct{}{}
						}
					} else if tr, ok := LookupTransformer("testB"); ok {
						out, err := tr("/path/to/file.txt")
						if err != nil || !strings.HasPrefix(out, "/alt/path/") {
							errs <- struct{}{}
						}
					}
				}
			}()
		}

		for range writers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for i := range 200 {
					if i%2 == 0 {
						SetDefaultRegistry(regB)
					} else {
						SetDefaultRegistry(regA)
					}
				}
			}()
		}

		wg.Wait()
		close(errs)

		failCount := 0

		for range errs {
			failCount++
		}

		So(failCount, ShouldEqual, 0)
	})

	Convey("LookupTransformer and DefaultRegistry handle nil default", t, func() {
		SetDefaultRegistry(nil)

		r := DefaultRegistry()
		So(r, ShouldBeNil)

		_, ok := LookupTransformer("anything")
		So(ok, ShouldBeFalse)
	})
}

func TestSetDefaultRegistryFromFile(t *testing.T) {
	Convey("SetDefaultRegistryFromFile can be called concurrently with lookups", t, func() {
		tmp, err := os.CreateTemp("", "ibackup-reg*.ini")
		So(err, ShouldBeNil)

		defer os.Remove(tmp.Name())

		_, err = tmp.WriteString(internal.TransformerConfigString)
		So(err, ShouldBeNil)
		So(tmp.Close(), ShouldBeNil)

		So(SetDefaultRegistryFromFile(tmp.Name()), ShouldBeNil)

		local := "/lustre/scratch118/humgen/projects/ddd/file.txt"

		var wg sync.WaitGroup

		errs := make(chan struct{}, 10000)
		readers, writers := 50, 10

		for range readers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range 500 {
					if tr, ok := LookupTransformer("humgen"); ok {
						out, err := tr(local)
						if err != nil || !strings.HasPrefix(out, "/humgen/projects/ddd/") {
							errs <- struct{}{}
						}
					} else {
						errs <- struct{}{}
					}
				}
			}()
		}

		for range writers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range 200 {
					if err := SetDefaultRegistryFromFile(tmp.Name()); err != nil {
						errs <- struct{}{}
					}
				}
			}()
		}

		wg.Wait()
		close(errs)

		failCount := 0

		for range errs {
			failCount++
		}

		So(failCount, ShouldEqual, 0)
	})
}

func TestPrefixTransformer(t *testing.T) {
	Convey("ParsePrefixTransformer correctly handles prefix transformer strings", t, func() {
		tr, ok := ParsePrefixTransformer("prefix=/local/path:/remote/path")
		So(ok, ShouldBeTrue)
		So(tr, ShouldNotBeNil)

		remote, err := tr("/local/path/file.txt")
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/remote/path/file.txt")

		remote, err = tr("/other/path/file.txt")
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/remote/path/other/path/file.txt")

		tr, ok = ParsePrefixTransformer("prefix=/local/path/remote/path")
		So(ok, ShouldBeFalse)
		So(tr, ShouldBeNil)

		tr, ok = ParsePrefixTransformer("humgen")
		So(ok, ShouldBeFalse)
		So(tr, ShouldBeNil)
	})

	Convey("LookupTransformer handles prefix transformers", t, func() {
		registry := NewTransformerRegistry()
		So(registry.Register("test", "Test transformer", `/path/to/(.+)`, "/new/path/$1"), ShouldBeNil)
		SetDefaultRegistry(registry)

		tr, ok := LookupTransformer("test")
		So(ok, ShouldBeTrue)
		So(tr, ShouldNotBeNil)

		remote, err := tr("/path/to/file.txt")
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/new/path/file.txt")

		tr, ok = LookupTransformer("prefix=/local/path:/remote/path")
		So(ok, ShouldBeTrue)
		So(tr, ShouldNotBeNil)

		remote, err = tr("/local/path/file.txt")
		So(err, ShouldBeNil)
		So(remote, ShouldEqual, "/remote/path/file.txt")

		tr, ok = LookupTransformer("nonexistent")
		So(ok, ShouldBeFalse)
		So(tr, ShouldBeNil)

		tr, ok = LookupTransformer("prefix=/local/path/remote/path")
		So(ok, ShouldBeFalse)
		So(tr, ShouldBeNil)
	})
}
