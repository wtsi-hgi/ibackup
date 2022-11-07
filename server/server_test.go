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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/go-resty/resty/v2"
	. "github.com/smartystreets/goconvey/convey"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/set"
)

func TestServer(t *testing.T) {
	exampleSet := &set.Set{
		Name:        "set1",
		Requester:   "jim",
		Transformer: "prefix=/local:/remote",
		Monitor:     false,
	}

	Convey("Given a Server", t, func() {
		logWriter := gas.NewStringLogger()
		s := New(logWriter)

		Convey("You can Start the Server", func() {
			certPath, keyPath, err := gas.CreateTestCert(t)
			So(err, ShouldBeNil)

			addr, dfunc, err := gas.StartTestServer(s, certPath, keyPath)
			So(err, ShouldBeNil)
			defer func() {
				errd := dfunc()
				So(errd, ShouldBeNil)
			}()

			client := resty.New()
			client.SetRootCertificate(certPath)

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, string) {
					return true, "1"
				})
				So(err, ShouldBeNil)

				token, errl := gas.Login(addr, certPath, exampleSet.Requester, "pass")
				So(errl, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				Convey("And then you can LoadSetDB and use client methods AddOrUpdateSet and GetSets", func() {
					path := createDBLocation(t)
					err = s.LoadSetDB(path)
					So(err, ShouldBeNil)

					err = AddOrUpdateSet(addr, certPath, token, exampleSet)
					So(err, ShouldBeNil)

					sets, err := GetSets(addr, certPath, token, exampleSet.Requester)
					So(err, ShouldBeNil)
					So(len(sets), ShouldEqual, 1)
					So(sets[0], ShouldResemble, exampleSet)

					_, err = GetSets(addr, certPath, token, "foo")
					So(err, ShouldNotBeNil)

					exampleSet.Requester = "foo"
					err = AddOrUpdateSet(addr, certPath, token, exampleSet)
					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("You can use the set endpoint", func() {
			response, err := putSet(s, nil)
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)
			So(logWriter.String(), ShouldContainSubstring, "[PUT /rest/v1/set")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

			Convey("And given a set database", func() {
				path := createDBLocation(t)

				Convey("You can add sets after calling LoadSetDB", func() {
					err = s.LoadSetDB(path)
					So(err, ShouldBeNil)

					response, err := putSet(s, exampleSet)
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)
					So(logWriter.String(), ShouldContainSubstring, "[PUT /rest/v1/set")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

					sets, err := s.db.GetAll()
					So(err, ShouldBeNil)
					So(len(sets), ShouldEqual, 1)
					So(sets[0], ShouldResemble, exampleSet)
				})
			})

			Convey("LoadSetDB fails on an invalid path", func() {
				err := s.LoadSetDB("/foo")
				So(err, ShouldNotBeNil)
			})
		})
	})
}

// createDBLocation creates a temporary location for to store a database and
// returns the path to the (non-existent) database file.
func createDBLocation(t *testing.T) string {
	t.Helper()

	tdir := t.TempDir()
	path := filepath.Join(tdir, "set.db")

	return path
}

// putSet does a test PUT of /rest/v1/set, with the given set converted to
// ?.
func putSet(s *Server, set *set.Set) (*httptest.ResponseRecorder, error) {
	response := httptest.NewRecorder()

	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(set)

	req, err := http.NewRequestWithContext(context.Background(), "PUT", EndPointSet, buf)
	if err != nil {
		return nil, err
	}

	s.Router().ServeHTTP(response, req)

	return response, nil
}

// decodeSetResult decodes the result of a set put.
// func decodeSetResult(response *httptest.ResponseRecorder) (string, error) {
// 	var result string
// 	err := json.NewDecoder(response.Body).Decode(&result)

// 	return result, err
// }
