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
	"net/http"

	"github.com/gin-gonic/gin"
	gas "github.com/wtsi-hgi/go-authserver"
)

const (
	setPath = "/set"

	// EndPointSet is the endpoint for making set queries if authorization
	// isn't implemented.
	EndPointSet = gas.EndPointREST + setPath

	// EndPointAuthSet is the endpoint for making set queries if authorization
	// is implemented.
	EndPointAuthSet = gas.EndPointAuth + setPath

	ErrNoSetDBDirFound = gas.Error("set database directory not found")
)

// LoadSetDB loads the given set.db or creates it if it doesn't exist, and adds
// the /rest/v1/set PUT and GET endpoint to the REST API. If you call
// EnableAuth() first, then these endpoints will be secured and be available at
// /rest/v1/auth/set.
//
// The set endpoint needs the x, y, z parameters, which correspond to arguments
// that set.X takes.
func (s *Server) LoadSetDB(path string) error {
	// s.treeMutex.Lock()
	// defer s.treeMutex.Unlock()

	// tree, err := dgut.NewTree(paths...)
	// if err != nil {
	// 	return err
	// }

	// s.tree = tree
	// s.dgutPaths = paths

	authGroup := s.AuthRouter()

	if authGroup == nil {
		s.Router().PUT(EndPointSet, s.putSet)
	} else {
		authGroup.PUT(EndPointAuthSet, s.putSet)
	}

	return nil
}

// putSet stores the given set information in the db and responds with the
// corresponding set ID. LoadSetDB() must already have been called. This is
// called when there is a PUT on /rest/v1/set or /rest/v1/auth/set.
func (s *Server) putSet(c *gin.Context) {
	// s.treeMutex.Lock()
	// defer s.treeMutex.Unlock()

	// dcss, err := s.tree.Where(dir, filter, convertSplitsValue(splits))
	// if err != nil {
	// 	c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

	// 	return
	// }

	c.IndentedJSON(http.StatusOK, "")
}
