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
	"context"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"time"

	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/db"
	"github.com/wtsi-hgi/ibackup/errs"
	"github.com/wtsi-npg/extendo/v2"
)

var (
	ErrUnknownTaskType = errors.New("unknown task type")
)

type queueDB interface {
	RegisterProcess() (*db.Process, error)
	PingProcess(process *db.Process) error
	ReserveTasks(process *db.Process, n int) *db.IterErr[*db.Task]
	TaskComplete(t *db.Task) error
	TaskFailed(t *db.Task) error
}

type Handler struct {
	handler *baton.Baton
	db      queueDB
	process *db.Process
	ctx     context.Context //nolint:containedctx
	stopFn  context.CancelCauseFunc

	remoteHardlinkPath string

	emptyFile      string
	emptyFileClose func()
}

func New(db queueDB, remoteHardlinkPath string) (*Handler, error) {
	btn, err := baton.GetBatonHandler()
	if err != nil {
		return nil, err
	}

	emptyFile, emptyFileClose, err := newEmptyFile()
	if err != nil {
		return nil, err
	}

	process, err := db.RegisterProcess()
	if err != nil {
		return nil, err
	}

	ctx, stopFn := context.WithCancelCause(context.Background())
	h := &Handler{
		handler:            btn,
		db:                 db,
		process:            process,
		ctx:                ctx,
		stopFn:             stopFn,
		remoteHardlinkPath: remoteHardlinkPath,
		emptyFile:          emptyFile,
		emptyFileClose:     emptyFileClose,
	}

	go h.ping()

	return h, nil
}

func (h *Handler) ping() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := h.db.PingProcess(h.process); err != nil {
				h.stopFn(err)

				return
			}
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *Handler) Stop() error {
	h.stopFn(nil)
	h.emptyFileClose()

	return context.Cause(h.ctx)
}

func (h *Handler) GetTasks(n int) ([]*db.Task, error) {
	if err := h.valid(); err != nil {
		return nil, err
	}

	tasks := h.db.ReserveTasks(h.process, n)
	collected := slices.Collect(tasks.Iter)

	return collected, tasks.Error
}

func (h *Handler) valid() error {
	select {
	case <-h.ctx.Done():
		return context.Cause(h.ctx)
	default:
		return nil
	}
}

func (h *Handler) HandleTask(task *db.Task) error { //nolint:gocyclo,funlen
	if err := h.valid(); err != nil {
		return err
	}

	if task.Type == db.QueueDisabled {
		return h.db.TaskComplete(task)
	}

	remoteStat, err := h.statPath(task.RemotePath)
	if err != nil {
		return err
	}

	task.InodePath = path.Join(h.remoteHardlinkPath, task.InodePath)

	inodeStat, err := h.statPath(task.InodePath)
	if err != nil {
		return err
	}

	switch task.Type {
	case db.QueueUpload:
		err = h.upload(task, remoteStat, inodeStat)
	case db.QueueRemoval:
		err = h.remove(task, remoteStat, inodeStat)
	default:
		err = ErrUnknownTaskType
	}

	if err != nil {
		task.Error = err.Error()

		return h.db.TaskFailed(task)
	}

	return h.db.TaskComplete(task)
}

func (h *Handler) statPath(remotePath string) (*baton.Stat, error) {
	stat, err := h.handler.StatWithMeta(remotePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil //nolint:nilnil
	}

	return stat, err
}

func (h *Handler) upload(task *db.Task, remote, inode *baton.Stat) error {
	if err := h.uploadInode(task, inode); err != nil {
		return err
	}

	return h.uploadRemote(task, remote)
}

func (h *Handler) uploadInode(task *db.Task, inode *baton.Stat) error {
	if inode == nil {
		if err := h.handler.Mkdir(path.Dir(task.InodePath)); err != nil {
			return err
		}
	}

	if inode == nil || task.Size != inode.Size || isNewer(task, inode) {
		if err := h.handler.Put(task.LocalPath, task.InodePath); err != nil {
			return err
		}
	}

	return h.updateMetadata(inode, task.InodePath,
		nil,
		[]extendo.AVU{{Attr: MetaKeyMtime, Value: time.Unix(task.MTime, 0).Format(time.RFC3339)}},
		[]extendo.AVU{{Attr: MetaKeyHardlink, Value: task.RemotePath}},
	)
}

func (h *Handler) uploadRemote(task *db.Task, remote *baton.Stat) error {
	if remote == nil { //nolint:nestif
		if err := h.handler.Mkdir(path.Dir(task.RemotePath)); err != nil {
			return err
		}

		if err := h.handler.Put(h.emptyFile, task.RemotePath); err != nil {
			return err
		}
	}

	return h.updateMetadata(remote, task.RemotePath,
		[]extendo.AVU{{Attr: MetaKeyDate, Value: time.Now().Truncate(time.Second).Format(time.RFC3339)}},
		getStatMetadataFromTask(task),
		getTaskMetadata(task),
	)
}

func isNewer(task *db.Task, stat *baton.Stat) bool {
	match := matchAVU(stat.Metadata, extendo.AVU{Attr: MetaKeyMtime})
	if match.Attr == "" {
		return time.Unix(task.MTime, 0).After(stat.MTime)
	}

	mtime, err := time.Parse(time.RFC3339, match.Value)
	if err != nil {
		return true
	}

	return time.Unix(task.MTime, 0).After(mtime)
}

func (h *Handler) remove(task *db.Task, remote, inode *baton.Stat) error {
	if err := h.removeRemote(task, remote); err != nil {
		return err
	}

	return h.removeInode(task, inode)
}

func (h *Handler) removeRemote(task *db.Task, remote *baton.Stat) error {
	if remote == nil {
		return nil
	}

	task.RemoteRefs--

	if task.RemoteRefs == 0 {
		if err := h.handler.RemoveFile(task.RemotePath); err != nil {
			return err
		}

		task.InodeRefs--

		return h.removeParentCollections(path.Dir(task.RemotePath))
	}

	return h.handler.RemoveAVUs(task.RemotePath, getSetSpecificMetadata(task, remote.Metadata))
}

func (h *Handler) removeParentCollections(path string) error {
	if err := h.handler.RemoveDir(path); err != nil {
		var dirNotEmpty errs.DirNotEmptyError

		if errors.As(err, &dirNotEmpty) {
			return nil
		}

		return errs.NewDirError(err.Error(), path)
	}

	return h.removeParentCollections(filepath.Dir(path))
}

func (h *Handler) removeInode(task *db.Task, inode *baton.Stat) error {
	if inode == nil {
		return nil
	}

	if task.InodeRefs == 0 {
		if err := h.handler.RemoveFile(task.InodePath); err != nil {
			return err
		}

		return h.removeParentCollections(path.Dir(task.InodePath))
	}

	return h.handler.RemoveAVUs(task.RemotePath, []extendo.AVU{
		{Attr: MetaKeyHardlink, Value: task.RemotePath},
	})
}
