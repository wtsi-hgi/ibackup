package internal

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/wtsi-hgi/ibackup/transformer"
)

func RegisterDefaultTransformers(t *testing.T) (transformer.PathTransformer, transformer.PathTransformer, transformer.PathTransformer) {
	t.Helper()

	genRe := `^/lustre/(scratch[^/]+)(/[^/]*)+?/([pP]rojects|teams|users)(_v2)?/([^/]+)/`

	So(transformer.Register("humgen", genRe, "/humgen/$3/$5/$1$4/"), ShouldBeNil)
	So(transformer.Register("gengen", genRe, "/humgen/gengen/$3/$5/$1$4/"), ShouldBeNil)
	So(transformer.Register("otar", genRe, "/humgen/open-targets/$3/$5/$1$4/"), ShouldBeNil)

	HumgenTransformer, err := transformer.MakePathTransformer("humgen")
	So(err, ShouldBeNil)

	GengenTransformer, err := transformer.MakePathTransformer("gengen")
	So(err, ShouldBeNil)

	OpentargetsTransformer, err := transformer.MakePathTransformer("otar")
	So(err, ShouldBeNil)

	return HumgenTransformer, GengenTransformer, OpentargetsTransformer
}
