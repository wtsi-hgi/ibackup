package db

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

//nolint:gochecknoglobals
var (
	simpleTransformer  = must(NewTransformer("prefix", `^/some/`, "/remote/"))
	complexTransformer = must(NewTransformer(
		"complex",
		`^/mount/(point[^/]+)(/[^/]*)+?/(projects|teams|users)(_v2)?/([^/]+)/`,
		"/backup/$3/$5/$1$4/",
	))
)

func must[T any](v T, err error) T { //nolint:ireturn
	if err != nil {
		panic(err)
	}

	return v
}

func TestTransformers(t *testing.T) {
	Convey("A transformer transforms paths", t, func() {
		for _, test := range [...]struct {
			Name        string
			Transformer *Transformer
			Input       string
			Valid       bool
			Output      string
		}{
			{
				"A simple path transformation",
				simpleTransformer,
				"/some/path/some/file",
				true,
				"/remote/path/some/file",
			},
			{
				"A simple path transformation that fails",
				simpleTransformer,
				"/dome/path/some/file",
				false,
				"",
			},
			{
				"A complex path transformation",
				complexTransformer,
				"/mount/point_1/something/projects_v2/awesome/file",
				true,
				"/backup/projects/awesome/point_1_v2/file",
			},
			{
				"A complex path transformation that fails",
				complexTransformer,
				"/some/other/path/some/file",
				false,
				"",
			},
		} {
			Convey(test.Name, func() {
				So(test.Transformer.Match(test.Input), ShouldEqual, test.Valid)

				transformed, err := test.Transformer.Transform(test.Input)
				if test.Valid {
					So(err, ShouldBeNil)
				} else {
					So(err, ShouldNotBeNil)
				}

				So(transformed, ShouldEqual, test.Output)
			})
		}
	})

	Convey("With a new database", t, func() {
		d := createTestDatabase(t)

		Convey("Adding sets only adds new transformers", func() {
			setA := &Set{
				Name:        "mySet",
				Requester:   "me",
				Transformer: complexTransformer,
				Description: "my first set",
			}

			setB := &Set{
				Name:        "my2ndSet",
				Requester:   "me",
				Transformer: complexTransformer,
				Description: "my second set",
			}

			setC := &Set{
				Name:        "mySet",
				Requester:   "you",
				Transformer: simpleTransformer,
				Description: "my first set",
				Reason:      Backup,
				ReviewDate:  time.Now().Truncate(time.Second).In(time.UTC),
				DeleteDate:  time.Now().Truncate(time.Second).In(time.UTC).Add(time.Hour),
				Metadata: Metadata{
					"some-extra-meta": "some-value",
				},
			}

			So(d.CreateSet(setA), ShouldBeNil)
			So(d.CreateSet(setB), ShouldBeNil)
			So(d.CreateSet(setC), ShouldBeNil)

			transformers := collectIter(t, iterRows(&d.DBRO, scanTransformers,
				"SELECT `id`, `transformer`, `regexp`, `replace` FROM `transformers`;"))
			So(transformers, ShouldResemble, []*Transformer{
				complexTransformer,
				setC.Transformer,
			})
		})

		Convey("You cannot have two remote files with different transformers", func() {
			prefixA, err := NewTransformer("prefix", "^/some/file/", "/some/remote/path/")
			So(err, ShouldBeNil)

			prefixB, err := NewTransformer("prefix", "^/some/local/file/", "/some/remote/path/")
			So(err, ShouldBeNil)

			setD := &Set{
				Name:        "mySet",
				Requester:   "you",
				Transformer: prefixA,
				Description: "my first set",
			}
			setE := &Set{
				Name:        "myOtherSet",
				Requester:   "you",
				Transformer: prefixA,
				Description: "my second set",
			}
			setF := &Set{
				Name:        "myFinalSet",
				Requester:   "you",
				Transformer: prefixB,
				Description: "my third set",
			}

			So(d.CreateSet(setD), ShouldBeNil)
			So(d.CreateSet(setE), ShouldBeNil)
			So(d.CreateSet(setF), ShouldBeNil)

			file := slices.Collect(genFiles(1))

			So(d.CompleteDiscovery(setD, slices.Values(file), noSeq[*File]), ShouldBeNil)
			So(d.CompleteDiscovery(setE, slices.Values(file), noSeq[*File]), ShouldBeNil)

			file[0].LocalPath = "/some/local/file/" + filepath.Base(file[0].LocalPath)

			So(d.CompleteDiscovery(setF, slices.Values(file), noSeq[*File]), ShouldEqual, ErrTransformerConflict)
		})
	})
}

func scanTransformers(scanner scanner) (*Transformer, error) {
	var (
		transformerID                                          int64
		transformerName, transformerRegexp, transformerReplace string
	)

	if err := scanner.Scan(&transformerID, &transformerName, &transformerRegexp, &transformerReplace); err != nil {
		return nil, err
	}

	trans, err := NewTransformer(transformerName, transformerRegexp, transformerReplace)
	if err != nil {
		return nil, err
	}

	trans.id = transformerID

	return trans, nil
}
