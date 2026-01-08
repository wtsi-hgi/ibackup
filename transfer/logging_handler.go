package transfer

import (
	"time"

	"github.com/wtsi-hgi/ibackup/internal/logger"
)

const irodsStillRunningLogFreq = 2 * time.Minute

type irodsSnapshotper interface {
	Snapshot(remote string) (exists bool, size uint64, checksum string, replicas int, replicateDetails []string, err error)
}

type loggingHandler struct {
	base   Handler
	logger logger.Logger
	ctx    []any
}

func (h *loggingHandler) WithLogCtx(ctx ...any) *loggingHandler {
	if len(ctx) == 0 {
		return h
	}

	combined := make([]any, 0, len(h.ctx)+len(ctx))
	combined = append(combined, h.ctx...)
	combined = append(combined, ctx...)

	return &loggingHandler{base: h.base, logger: h.logger, ctx: combined}
}

func (h *loggingHandler) EnsureCollection(collection string) (err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods ensure collection",
		irodsStillRunningLogFreq,
		append(h.ctx, "collection", collection)...,
	)

	defer func() { finish(err) }()

	return h.base.EnsureCollection(collection)
}

func (h *loggingHandler) CollectionsDone() (err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods collections done",
		0,
		h.ctx...,
	)

	defer func() { finish(err) }()

	return h.base.CollectionsDone()
}

func (h *loggingHandler) Stat(remote string) (exists bool, meta map[string]string, err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods stat",
		irodsStillRunningLogFreq,
		append(h.ctx, "remote", remote)...,
	)

	defer func() { finish(err, "exists", exists, "keys", len(meta)) }()

	exists, meta, err = h.base.Stat(remote)

	return exists, meta, err
}

func (h *loggingHandler) Put(local, remote string) (err error) {
	h.logSnapshot("pre-put", remote)

	finish := logger.StartOperation(
		h.logger,
		"irods put",
		irodsStillRunningLogFreq,
		append(h.ctx, "local", local, "remote", remote)...,
	)

	defer func() {
		finish(err)

		if err != nil {
			h.logSnapshot("post-put (error)", remote)
		} else {
			h.logSnapshot("post-put", remote)
		}
	}()

	return h.base.Put(local, remote)
}

func (h *loggingHandler) Get(local, remote string) (err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods get",
		irodsStillRunningLogFreq,
		append(h.ctx, "local", local, "remote", remote)...,
	)

	defer func() { finish(err) }()

	return h.base.Get(local, remote)
}

func (h *loggingHandler) RemoveMeta(path string, meta map[string]string) (err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods remove meta",
		irodsStillRunningLogFreq,
		append(h.ctx, "path", path, "keys", len(meta))...,
	)

	defer func() { finish(err) }()

	return h.base.RemoveMeta(path, meta)
}

func (h *loggingHandler) AddMeta(path string, meta map[string]string) (err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods add meta",
		irodsStillRunningLogFreq,
		append(h.ctx, "path", path, "keys", len(meta))...,
	)

	defer func() { finish(err) }()

	return h.base.AddMeta(path, meta)
}

func (h *loggingHandler) Cleanup() {
	if h.base != nil {
		h.base.Cleanup()
	}
}

func (h *loggingHandler) GetMeta(path string) (meta map[string]string, err error) {
	finish := logger.StartOperation(
		h.logger,
		"irods get meta",
		irodsStillRunningLogFreq,
		append(h.ctx, "path", path)...,
	)

	defer func() { finish(err, "keys", len(meta)) }()

	meta, err = h.base.GetMeta(path)

	return meta, err
}

func (h *loggingHandler) logSnapshot(label, remote string) {
	if h.logger == nil {
		return
	}

	sp, ok := h.base.(irodsSnapshotper)
	if !ok {
		return
	}

	exists, size, checksum, replicas, details, err := sp.Snapshot(remote)
	if err != nil {
		h.logSnapshotError(label, remote, err)

		return
	}

	if !exists {
		h.logSnapshotNotExists(label, remote)

		return
	}

	h.logSnapshotExists(label, remote, size, checksum, replicas, details)
}

func (h *loggingHandler) logSnapshotError(label, remote string, err error) {
	h.logger.Warn(
		"irods snapshot failed",
		append(h.ctx, "label", label, "remote", remote, "err", err)...,
	)
}

func (h *loggingHandler) logSnapshotNotExists(label, remote string) {
	h.logger.Info(
		"irods snapshot",
		append(h.ctx, "label", label, "remote", remote, "exists", false)...,
	)
}

func (h *loggingHandler) logSnapshotExists(
	label string,
	remote string,
	size uint64,
	checksum string,
	replicas int,
	details []string,
) {
	// Keep the info-level snapshot compact; replica details remain at Debug.
	h.logger.Info(
		"irods snapshot",
		append(
			h.ctx,
			"label", label,
			"remote", remote,
			"exists", true,
			"size", size,
			"checksum", checksum,
			"replicas", replicas,
		)...,
	)
	h.logger.Debug(
		"irods snapshot details",
		append(h.ctx, "label", label, "remote", remote, "replicate_details", details)...,
	)
}

type logCtxHandler interface {
	WithLogCtx(ctx ...any) *loggingHandler
}
