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

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
)

func TestServer(t *testing.T) {
	_, uid := gas.GetUser(t)

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can Start the Server", func() {
			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			_, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			client := resty.New()
			client.SetRootCertificate(certPath)

			Convey("The jwt endpoint works after enabling it", func() {
				// err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
				// 	returnUID := uid

				// 	if u == "user" {
				// 		returnUID = "-1"
				// 	}

				// 	return true, returnUID
				// })
				// So(err, ShouldBeNil)

				// token, errl := gas.Login(addr, certPath, username, "pass")
				// So(errl, ShouldBeNil)
				// r := gas.NewAuthenticatedClientRequest(addr, certPath, token)

				// tokenBadUID, errl := gas.Login(addr, certPath, "user", "pass")
				// So(errl, ShouldBeNil)
				// So(tokenBadUID, ShouldNotBeBlank)
			})
		})

		Convey("You can use the set endpoint", func() {
			response, err := querySet(s, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[PUT /rest/v1/set")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			Convey("And given a set database", func() {
				path, err := createExampleDB(t, uid)
				So(err, ShouldBeNil)

				// sdb, err := set.NewDB(path)
				// So(err, ShouldBeNil)

				Convey("You can add sets after calling LoadSetDB", func() {
					err = s.LoadSetDB(path)
					So(err, ShouldBeNil)

					response, err := querySet(s, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[PUT /rest/v1/set")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					result, err := decodeSetResult(response)
					So(err, ShouldBeNil)
					So(result, ShouldResemble, "foo")
				})
			})

			Convey("LoadSetDB fails on an invalid path", func() {
				err := s.LoadSetDB("/foo")
				So(err, ShouldNotBeNil)
			})
		})
	})
}

// querySet does a test PUT of /rest/v1/set, with extra appended (start it
// with ?).
func querySet(s *Server, extra string) (*httptest.ResponseRecorder, error) {
	response := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(context.Background(), "PUT", EndPointSet+extra, nil)
	if err != nil {
		return nil, err
	}

	s.Router().ServeHTTP(response, req)

	return response, nil
}

// decodeSetResult decodes the result of a set query.
func decodeSetResult(response *httptest.ResponseRecorder) (string, error) {
	var result string
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}

// createExampleDB creates a temporary set.db from some example data that uses
// the given uid, and returns the path to the database file.
func createExampleDB(t *testing.T, uid string) (string, error) {
	t.Helper()

	// tdir := t.TempDir()
	// path := filepath.Join(tdir, "set.db")

	// setData := exampleSetData(uid)
	// data := strings.NewReader(setData)
	// db := set.NewDB(path, setData)

	// err = db.Store(data, 20)

	// return path, err

	return "", nil
}

// exampleSetData is some example set data that uses the given uid.
func exampleSetData(uid string) string {
	data := `xxx
`

	data = strings.ReplaceAll(data, "z", uid)

	return data
}
