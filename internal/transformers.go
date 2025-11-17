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

package internal

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey" //nolint:revive,staticcheck
	"github.com/wtsi-hgi/ibackup/transformer"
)

// RegisterDefaultTransformers registers three named transformers, 'humgen',
// 'gengen', and 'otar', returning each as a PathTransformer.
func RegisterDefaultTransformers(t *testing.T) (transformer.PathTransformer, transformer.PathTransformer, transformer.PathTransformer) { //nolint:lll
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
