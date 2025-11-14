package transformer

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTransformers(t *testing.T) {
	Convey("A transformer transforms paths", t, func() {
		So(Register(
			"complex",
			`^/mount/(point[^/]+)(/[^/]*)+?/(projects|teams|users)(_v2)?/([^/]+)/`,
			"/backup/$3/$5/$1$4/",
		), ShouldBeNil)

		for _, test := range [...]struct {
			Name        string
			Transformer string
			Input       string
			Valid       bool
			Output      string
		}{
			{
				"A simple path transformation",
				"prefix=/some/:/remote/",
				"/some/path/some/file",
				true,
				"/remote/path/some/file",
			},
			{
				"A simple path transformation that should fail but doesn't",
				"prefix=/some/:/remote/",
				"/dome/path/some/file",
				true,
				"/remote/dome/path/some/file",
			},
			{
				"A complex path transformation",
				"complex",
				"/mount/point_1/something/projects_v2/awesome/file",
				true,
				"/backup/projects/awesome/point_1_v2/file",
			},
			{
				"A complex path transformation that fails",
				"complex",
				"/some/other/path/some/file",
				false,
				"",
			},
		} {
			Convey(test.Name, func() {
				transformed, err := Transform(test.Transformer, test.Input)
				if test.Valid {
					So(err, ShouldBeNil)
				} else {
					So(err, ShouldNotBeNil)
				}

				So(transformed, ShouldEqual, test.Output)
			})
		}
	})
}
