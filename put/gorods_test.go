/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package put

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPutGoRODS(t *testing.T) {
	h, err := GetGoRODSHandler()
	if err != nil {
		t.Logf("GetGoRODSHandler error: %s", err)
		SkipConvey("Skipping GoRODS tests since couldn't start it", t, func() {})

		return
	}

	rootCollection := os.Getenv("IBACKUP_TEST_COLLECTION")
	if rootCollection == "" {
		SkipConvey("Skipping gic tests since IBACKUP_TEST_COLLECTION is not defined", t, func() {})

		return
	}

	Convey("Given Requests and a GoRODS Handler, you can make a new Putter", t, func() {
		requests, _ := makeRequests(t, rootCollection)

		p, err := New(h, requests)
		So(err, ShouldBeNil)
		So(p, ShouldNotBeNil)

		SkipConvey("CreateCollections() creates the needed collections", func() {

		})
	})
}
