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

package db

import (
	"database/sql"
	"path"
	"strconv"
	"time"
)

type QueueType uint8

const (
	QueueDisabled QueueType = iota
	QueueUpload
	QueueRemoval
)

const maxRetries = 3

type Process struct {
	id int64
}

type Task struct {
	id          int64
	process     int64
	LocalPath   string
	RemotePath  string
	InodePath   string
	Size        uint64
	MTime       int64
	SymlinkDest string
	Type        QueueType
	Requester   string
	SetName     string
	Owner       string
	Group       string
	Reason      Reason
	ReviewDate  time.Time
	DeleteDate  time.Time
	Metadata    Metadata
	Error       string
	Skipped     bool
	RemoteRefs  uint64
	InodeRefs   uint64
}

func (d *DB) RegisterProcess() (*Process, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	r, err := tx.Exec(createProcess)
	if err != nil {
		return nil, err
	}

	id, err := r.LastInsertId()
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &Process{id: id}, nil
}

func (d *DB) PingProcess(process *Process) error {
	return d.exec(updateProcessPing, process.id)
}

func (d *DBRO) CountProcesses() (int64, error) {
	var count int64

	if err := d.db.QueryRow(getProcessCount).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

func (d *DB) RemoveStaleProcesses() error {
	return d.exec(deleteStaleProcesses)
}

func (d *DB) ReserveTasks(process *Process, n int) *IterErr[*Task] {
	if err := d.reserveQueuedTasks(process, n); err != nil {
		return &IterErr[*Task]{Iter: noSeq[*Task], Error: err}
	}

	return d.getReservedTasks(process)
}

func (d *DB) ReleaseTasks(process *Process) error {
	return d.exec(releaseQueuedTask, process.id)
}

func (d *DB) reserveQueuedTasks(process *Process, n int) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	for range n {
		_, err = tx.Exec(holdQueuedTask, process.id)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DBRO) getReservedTasks(process *Process) *IterErr[*Task] {
	return iterRows(d, scanTask(process.id), getQueuedTasks, process.id)
}

func scanTask(process int64) func(scanner) (*Task, error) { //nolint:funlen
	return func(s scanner) (*Task, error) {
		t := new(Task)

		var (
			reviewDate, deleteDate sql.NullTime
			mountPoint             string
			inode                  uint64
			bTime                  int64
		)

		if err := s.Scan(
			&t.id,
			&t.Type,
			&t.LocalPath,
			&t.RemotePath,
			&mountPoint,
			&inode,
			&bTime,
			&t.Size,
			&t.MTime,
			&t.SymlinkDest,
			&t.Owner,
			&t.Group,
			&t.Requester,
			&t.SetName,
			&t.Reason,
			&reviewDate,
			&deleteDate,
			&t.Metadata,
			&t.RemoteRefs,
			&t.InodeRefs,
		); err != nil {
			return nil, err
		}

		t.InodePath = inodePath(mountPoint, inode, bTime)
		t.ReviewDate = reviewDate.Time
		t.DeleteDate = deleteDate.Time
		t.process = process

		return t, nil
	}
}

func inodePath(mountPoint string, inode uint64, bTime int64) string {
	if bTime == 0 {
		return path.Join(mountPoint, strconv.FormatUint(inode, 10))
	}

	return path.Join(mountPoint, "i"+strconv.FormatUint(inode, 10), strconv.FormatInt(bTime, 10))
}

func (d *DB) TaskComplete(t *Task) error { //nolint:gocyclo
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if t.Type == QueueUpload && t.Skipped {
		if _, err = tx.Exec(updateQueuedSkipped, t.id, t.process, t.Type); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(deleteQueued, t.id, t.process, t.Type); err != nil {
		return err
	}

	if t.Type == QueueRemoval {
		if _, err := tx.Exec(deleteRemoteFileWhenNotRefd); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DB) TaskFailed(t *Task) error {
	return d.exec(updateQueuedFailed, t.Error, t.id, t.process, t.Type)
}

func (d *DB) RetrySetTasks(set *Set) error {
	return d.exec(queueRetry, set.id)
}

func (d *DBRO) CountTasks() (int64, int64, error) {
	var total, held int64

	if err := d.db.QueryRow(getTasksCounts).Scan(&total, &held); err != nil {
		return 0, 0, err
	}

	return total, held, nil
}
