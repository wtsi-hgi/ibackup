package db

import (
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
				"SELECT `transformer`, `regexp`, `replace` FROM `transformers`;"))
			So(transformers, ShouldResemble, []*Transformer{
				complexTransformer,
				setC.Transformer,
			})
		})
	})
}

func scanTransformers(scanner scanner) (*Transformer, error) {
	var transformerName, transformerRegexp, transformerReplace string

	if err := scanner.Scan(&transformerName, &transformerRegexp, &transformerReplace); err != nil {
		return nil, err
	}

	return NewTransformer(transformerName, transformerRegexp, transformerReplace)
}
