package db

type QueueType uint8

const (
	QueueDisabled QueueType = iota
	QueueUpload
	QueueRemoval
)

type Process struct {
	id int64
}

type Task struct {
	id         int64
	LocalPath  string
	RemotePath string
	UploadPath string
	Type       QueueType
	Error      string
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
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	_, err = tx.Exec(updateProcessPing, process.id)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) ReserveTasks(process *Process, n int) *IterErr[*Task] {
	if err := d.reserveQueuedTasks(process, n); err != nil {
		return &IterErr[*Task]{Iter: noSeq[*Task], Error: err}
	}

	return d.getReservedTasks(process)
}

func (d *DB) ReleaseTasks(process *Process) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	_, err = tx.Exec(releaseQueuedTask, process.id)
	if err != nil {
		return err
	}

	return tx.Commit()
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
	return iterRows(d, scanTask, getQueuedTasks, process.id)
}

func scanTask(s scanner) (*Task, error) {
	t := new(Task)

	if err := s.Scan(
		&t.id,
		&t.Type,
		&t.LocalPath,
		&t.RemotePath,
		&t.UploadPath,
	); err != nil {
		return nil, err
	}

	return t, nil
}

func (d *DB) TaskComplete(t *Task) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec(deleteQueued, t.id, t.Type); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) TaskFailed(t *Task) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec(updateQueuedFailed, t.Error, t.id, t.Type); err != nil {
		return err
	}

	return tx.Commit()
}
