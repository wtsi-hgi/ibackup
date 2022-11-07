/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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

// package server provides a web server for a REST API and website.

package server

import (
	"io"

	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/set"
)

// Server is used to start a web server that provides a REST API to the setdb
// package's database, and a website that displays the information nicely.
type Server struct {
	gas.Server
	db *set.DB
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the given io.Writer, which could for example be syslog using the
// log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(logWriter io.Writer) *Server {
	s := &Server{
		Server: *gas.New(logWriter),
	}

	s.SetStopCallBack(s.stop)

	return s
}

// stop is called when the server is Stop()ped, cleaning up our additional
// properties.
func (s *Server) stop() {
	if s.db == nil {
		return
	}

	err := s.db.Close()
	if err != nil {
		s.Logger.Printf("database close failed: %s", err)
	}
}
