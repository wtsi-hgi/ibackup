/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
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

package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseIUserInfo(t *testing.T) {
	Convey("parseIUserInfo returns the iRODS user's name and groups", t, func() {
		out := []byte(`name: ibackup
id: 10017
type: rodsuser
zone: testZone
info: 
comment: 
create time: 01791448352: 2026-10-08.09:32:32
modify time: 01791448352: 2026-10-08.09:32:32
member of group: ibackup
member of group: public
`)

		info, err := parseIUserInfo(out)
		So(err, ShouldBeNil)
		So(info.Name, ShouldEqual, "ibackup")
		So(info.Groups, ShouldResemble, []string{"ibackup", "public"})
	})

	Convey("parseIUserInfo fails if the output has no valid name", t, func() {
		for _, out := range []string{
			"id: 10017\nmember of group: public\n",
			"name: \n",
			"name: bad name\n",
		} {
			_, err := parseIUserInfo([]byte(out))
			So(err, ShouldWrap, errIUserInfoNoName)
		}
	})
}

func TestGrantOwn(t *testing.T) {
	Convey("GrantOwn runs ichmod to give a user own access", t, func() {
		dir := t.TempDir()
		record := filepath.Join(dir, "record")
		script := "#!/bin/sh\necho \"$IRODS_ENVIRONMENT_FILE $*\" > " + record + "\n"
		So(os.WriteFile(filepath.Join(dir, "ichmod"), []byte(script), 0700), ShouldBeNil) //nolint:gosec

		t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
		t.Setenv("IRODS_ENVIRONMENT_FILE", "/user.json")

		icmd := &ICommander{logger: t, timeout: time.Minute, maxAttempts: 1, backoff: time.Millisecond}

		recorded := func() string {
			b, err := os.ReadFile(record)
			So(err, ShouldBeNil)

			return strings.TrimSpace(string(b))
		}

		Convey("as the test user by default", func() {
			t.Setenv(IRODSAdminEnvKey, "")

			_, err := icmd.GrantOwn("someone", "/zone/obj")
			So(err, ShouldBeNil)
			So(recorded(), ShouldEqual, "/user.json own someone /zone/obj")
		})

		Convey("as the iRODS admin in admin mode if an admin environment is configured", func() {
			t.Setenv(IRODSAdminEnvKey, "/admin.json")

			_, err := icmd.GrantOwn("someone", "/zone/obj")
			So(err, ShouldBeNil)
			So(recorded(), ShouldEqual, "/admin.json -M own someone /zone/obj")
		})
	})
}
