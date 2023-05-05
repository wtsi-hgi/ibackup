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
	"os/user"
	"strconv"
	"syscall"
	"time"
)

const (
	MetaNamespace    = "ibackup:"
	metaKeyMtime     = MetaNamespace + "mtime"      // mtime of source file, 1sec truncated UTC RFC 3339
	metaKeyOwner     = MetaNamespace + "owner"      // a username
	metaKeyGroup     = MetaNamespace + "group"      // a unix group name
	metaKeyDate      = MetaNamespace + "date"       // date upload initiated, 1sec truncated UTC RFC 3339
	metaKeyRequester = MetaNamespace + "requesters" // a comma sep list of usernames of the people who reqested the backup
	metaKeySets      = MetaNamespace + "sets"       // a comma sep list of backup set names this file belongs to
	ErrStatFailed    = "stat of local path returned strange results"
)

type ObjectInfo struct {
	Exists bool
	Size   uint64
	Meta   map[string]string
}

// Stat stats localPath like os.Stat(), but also returns information about the
// file in ObjectInfo Meta (mtime, owner and group information).
func Stat(localPath string) (*ObjectInfo, error) {
	fi, err := os.Stat(localPath)
	if err != nil {
		return nil, err
	}

	mtime, err := timeToMeta(fi.ModTime())
	if err != nil {
		return nil, err
	}

	user, group, err := getUserAndGroupFromFileInfo(fi, localPath)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Exists: true,
		Size:   uint64(fi.Size()),
		Meta: map[string]string{
			metaKeyMtime: mtime,
			metaKeyOwner: user,
			metaKeyGroup: group,
		},
	}, nil
}

// timeToMeta converts a time to a string suitable for storing as metadata, in
// a way that ObjectInfo.ModTime() will understand and be able to convert back
// again.
func timeToMeta(t time.Time) (string, error) {
	b, err := t.UTC().Truncate(time.Second).MarshalText()
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// getUserAndGroupFromFileInfo returns the username and group name from the
// given FileInfo.
func getUserAndGroupFromFileInfo(fi os.FileInfo, localPath string) (string, string, error) {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", "", Error{ErrStatFailed, localPath}
	}

	u, err := user.LookupId(strconv.Itoa(int(stat.Uid)))
	if err != nil {
		return "", "", err
	}

	gid := strconv.Itoa(int(stat.Gid))

	g, err := user.LookupGroupId(gid)
	if err != nil {
		g = &user.Group{
			Name: gid,
		}
	}

	return u.Username, g.Name, nil
}

// HasSameModTime tells you if this ObjectInfo has the same ModTime() as the
// given one. Instead of comparing metadata strings, tries to convert them to
// times in a way that a comparison would be meaningful, in case the string
// was not generated by us.
func (o *ObjectInfo) HasSameModTime(as *ObjectInfo) bool {
	return o.ModTime().UTC().Truncate(time.Second) == as.ModTime().UTC().Truncate(time.Second)
}

// ModTime converts our mtime metadata key value string to a time.Time. Returns
// time zero if the key isn't present or convertable.
func (o *ObjectInfo) ModTime() time.Time {
	t := time.Time{}

	s, exists := o.Meta[metaKeyMtime]
	if !exists || s == "" {
		return t
	}

	t.UnmarshalText([]byte(s)) //nolint:errcheck

	return t
}
