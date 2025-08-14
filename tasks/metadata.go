/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package tasks

import (
	"strings"
	"time"

	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-npg/extendo/v2"
)

const (
	MetaNamespace         = "ibackup:"
	MetaUserNamespace     = MetaNamespace + "user:"          // prefix for user-supplied metadata keys
	MetaKeyMtime          = MetaNamespace + "mtime"          // mtime of source file, 1sec truncated UTC RFC 3339
	MetaKeyOwner          = MetaNamespace + "owner"          // a username
	MetaKeyGroup          = MetaNamespace + "group"          // a unix group name
	MetaKeyDate           = MetaNamespace + "date"           // date upload initiated, 1sec truncated UTC RFC 3339
	MetaKeyReason         = MetaNamespace + "reason"         // storage reason: backup|archive|quarantine
	MetaKeyReview         = MetaNamespace + "review"         // a date for review
	MetaKeyRemoval        = MetaNamespace + "removal"        // a date for removal
	MetaKeySet            = MetaNamespace + "set"            // requester (Value) and set name (Unit)
	MetaKeySymlink        = MetaNamespace + "symlink"        // symlink destination if file is a symlink
	MetaKeyRemoteHardlink = MetaNamespace + "remotehardlink" // iRODS path that contains the data for this hardlink.
	MetaKeyHardlink       = MetaNamespace + "hardlink"       // remotePaths that link to the inode
)

func getMetadataFromStat(s *baton.Stat) []extendo.AVU {
	if s == nil {
		return nil
	}

	return s.Metadata
}

func getStatMetadataFromTask(task *db.Task) []extendo.AVU {
	return []extendo.AVU{
		{Attr: MetaKeyMtime, Value: time.Unix(task.MTime, 0).Format(time.RFC3339)},
		{Attr: MetaKeyOwner, Value: task.Owner},
		{Attr: MetaKeyGroup, Value: task.Group},
		{Attr: MetaKeySymlink, Value: task.SymlinkDest},
		{Attr: MetaKeyRemoteHardlink, Value: task.InodePath},
	}
}

func getTaskMetadata(task *db.Task) []extendo.AVU {
	if strings.HasPrefix(task.Requester, "\x00") || strings.HasPrefix(task.SetName, "\x00") {
		return nil
	}

	requesterSet := task.Requester + " " + task.SetName

	avus := append(make([]extendo.AVU, 0, 4+len(task.Metadata)), //nolint:mnd
		extendo.AVU{Attr: MetaKeySet, Value: task.Requester, Units: task.SetName},
		extendo.AVU{Attr: MetaKeyReason, Value: task.Reason.String(), Units: requesterSet},
		extendo.AVU{Attr: MetaKeyReview, Value: task.ReviewDate.Format(time.RFC3339), Units: requesterSet},
		extendo.AVU{Attr: MetaKeyRemoval, Value: task.DeleteDate.Format(time.RFC3339), Units: requesterSet},
	)

	for key, value := range task.Metadata {
		avus = append(avus, extendo.AVU{Attr: MetaUserNamespace + key, Value: value, Units: requesterSet})
	}

	return avus
}

func getSetSpecificMetadata(task *db.Task, stat []extendo.AVU) []extendo.AVU {
	requesterSet := task.Requester + " " + task.SetName

	var avus []extendo.AVU

	for _, avu := range stat {
		if avu.Units == requesterSet ||
			avu.Attr == MetaKeySet && avu.Value == task.Requester && avu.Units == task.SetName { //nolint:whitespace
			avus = append(avus, avu)
		}
	}

	return avus
}

func resolveMetadataUpdate(got, needAs, needAVs, needAVUs []extendo.AVU) ([]extendo.AVU, []extendo.AVU) { //nolint:gocognit,gocyclo,lll
	var toSet, toRemove []extendo.AVU

	for _, as := range needAs {
		if matchAVU(got, extendo.AVU{Attr: as.Attr}).Attr == "" {
			toSet = append(toSet, as)
		}
	}

	for _, avs := range needAVs {
		if match := matchAVU(got, extendo.AVU{Attr: avs.Attr}); match.Value != avs.Value {
			toSet = append(toSet, avs)

			if match.Attr != "" {
				toRemove = append(toRemove, match)
			}
		}
	}

	for _, avus := range needAVUs {
		if matchAVU(got, extendo.AVU{Attr: avus.Attr, Value: avus.Value, Units: avus.Units}).Attr == "" {
			toSet = append(toSet, avus)
		}
	}

	return toSet, toRemove
}

func matchAVU(list []extendo.AVU, match extendo.AVU) extendo.AVU {
	for _, avu := range list {
		if avu.Attr == match.Attr &&
			(match.Value == "" || avu.Attr == match.Value) &&
			(match.Units == "" || avu.Units == match.Units) { //nolint:whitespace
			return avu
		}
	}

	return extendo.AVU{}
}

func (h *Handler) updateMetadata(stat *baton.Stat, remote string, needAs, needAvs, needAVUs []extendo.AVU) error {
	toAdd, toRemove := resolveMetadataUpdate(getMetadataFromStat(stat), needAs, needAvs, needAVUs)

	if err := h.handler.AddAVUs(remote, toAdd); err != nil {
		return err
	}

	return h.handler.RemoveAVUs(remote, toRemove)
}
