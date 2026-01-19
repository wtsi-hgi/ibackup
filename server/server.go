/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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

// package server provides a web server for a REST API and website.

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gammazero/workerpool"
	"github.com/inconshreveable/log15"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/remove"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-hgi/ibackup/transfer"
)

const (
	ErrNoLogger             = gas.Error("an http logger must be configured")
	ErrIncorrectTypeInQueue = gas.Error("incorrect data type in queue")

	// workerPoolSizeDir is the max number of directory walks we'll do
	// concurrently during discovery; each of those walks in turn operate on 16
	// subdirs concurrently.
	workerPoolSizeDir = 3

	// connectTimeout is the timeout for connecting to wr for job submission.
	connectTimeout = 10 * time.Second

	repGroup                = "ibackup_server_put"
	reqGroup                = "ibackup_server"
	reqRAM                  = 1024
	reqTime                 = 8 * time.Hour
	jobRetries        uint8 = 3
	jobLimitGroup           = "irods"
	racRetriggerDelay       = 1 * time.Minute

	retryDelay = 5 * time.Second

	maxRememberedRequestLogs = 100000
)

// Config configures the server.
type Config struct {
	// HTTPLogger is used to log all HTTP requests. This is required.
	HTTPLogger io.Writer

	// StillRunningMsgFreq is the time between slack messages being sent about
	// the server still running.
	StillRunningMsgFreq time.Duration

	// Slacker is used to send messages to a slack channel.
	Slacker set.Slacker

	// SlackMessageDebounce is the minimum time between slack upload count
	// messages. Default value means send unlimited messages, which will likely
	// result in slack restricting messages itself.
	SlackMessageDebounce time.Duration

	// ReadOnly disables monitoring, discovery, and database modifications.
	ReadOnly bool
	// StorageHandler is used to interact with the storage system, e.g. iRODS.
	StorageHandler remove.Handler

	// TrashLifespan is the duration that trash is kept.
	TrashLifespan time.Duration

	// FailedUploadRetryDelay is the delay applied to failed upload requests
	// before they are retried.
	//
	// A value of 0 means retry immediately.
	FailedUploadRetryDelay time.Duration

	// ReplicaLogging enables extra baton calls on clients to determine replica
	// numbers before/after uploads. This is passed through to clients via the
	// upload requests they receive.
	ReplicaLogging bool

	// HungDebugTimeout enables server-side hung debugging.
	//
	// When > 0, the server will periodically check for signs of stuck uploads and
	// (rate-limited) log a summary plus a goroutine dump.
	//
	// A value of 0 disables this feature entirely.
	HungDebugTimeout time.Duration
}

// Server is used to start a web server that provides a REST API to the setdb
// package's database, and a website that displays the information nicely.
type Server struct {
	gas.Server
	db                     *set.DB
	numClients             int
	numRequestsCache       []int
	cacheMu                sync.Mutex
	dirPoolMu              sync.Mutex
	dirPool                *workerpool.WorkerPool
	queue                  *queue.Queue
	removeQueue            *queue.Queue
	trashLifespan          time.Duration
	sched                  *client.Scheduler
	putCmd                 string
	req                    *jqs.Requirements
	remoteHardlinkLocation string
	statusUpdateCh         chan *fileStatusPacket
	monitor                *Monitor
	slacker                set.Slacker
	stillRunningMsgFreq    time.Duration
	serverAliveCh          chan bool
	uploadTracker          *uploadTracker
	failedUploadRetryDelay time.Duration
	replicaLogging         bool

	hungDebugTimeout    time.Duration
	hungDebugStopCh     chan struct{}
	hungDebugLastLog    atomic.Int64
	hungDebugLastStatus atomic.Int64
	hungDebugLastRID    atomic.Value // string

	readOnly       bool
	storageHandler remove.Handler

	mapMu               sync.RWMutex
	creatingCollections map[string]bool
	iRODSTracker        *iRODSTracker
	clientQueue         *queue.Queue

	requestLogMu   sync.Mutex
	requestLogged  map[string]struct{}
	requestLogRIDs []string

	discoveryCoordinator *discoveryCoordinator
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the required configured io.Writer, which could for example be
// syslog using the log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(conf Config) (*Server, error) { //nolint:funlen
	if conf.HTTPLogger == nil {
		return nil, ErrNoLogger
	}

	retryDelayToUse := max(conf.FailedUploadRetryDelay, 0)

	s := &Server{
		Server:                 *gas.New(conf.HTTPLogger),
		numClients:             1,
		dirPool:                workerpool.New(workerPoolSizeDir),
		queue:                  queue.New(context.Background(), "put"),
		removeQueue:            queue.New(context.Background(), "remove"),
		trashLifespan:          conf.TrashLifespan,
		creatingCollections:    make(map[string]bool),
		slacker:                conf.Slacker,
		stillRunningMsgFreq:    conf.StillRunningMsgFreq,
		uploadTracker:          newUploadTracker(conf.Slacker, conf.SlackMessageDebounce),
		failedUploadRetryDelay: retryDelayToUse,
		replicaLogging:         conf.ReplicaLogging,
		hungDebugTimeout:       conf.HungDebugTimeout,
		iRODSTracker:           newiRODSTracker(conf.Slacker, conf.SlackMessageDebounce),
		clientQueue:            queue.New(context.Background(), "client"),
		readOnly:               conf.ReadOnly,
		storageHandler:         conf.StorageHandler,
		requestLogged:          make(map[string]struct{}),

		discoveryCoordinator: newDiscoveryCoordinator(),
	}

	// Ensure the atomic.Value is usable before any stores.
	s.hungDebugLastRID.Store("")

	s.clientQueue.SetTTRCallback(s.clientTTRC)
	s.SetStopCallBack(s.stop)
	s.Server.Router().Use(gas.IncludeAbortErrorsInBody)

	if conf.ReadOnly {
		return s, nil
	}

	s.monitor = NewMonitor(s.monitorCB)

	return s, nil
}

func (s *Server) monitorCB(given *set.Set) {
	if err := s.discoverSet(given, false); err != nil {
		s.Logger.Printf("error discovering set during monitoring: %s", err)
	}
}

// Start logs to slack that the server has been started and then calls
// gas.Server.Start().
func (s *Server) Start(addr, certFile, keyFile string) error {
	s.sendSlackMessage(slack.Success, "server started")

	return s.Server.Start(addr, certFile, keyFile)
}

// StartACME logs to slack that the server has been started and then calls
// gas.Server.StartACME().
func (s *Server) StartACME(addr, acmeURL, cacheDir string) error {
	s.sendSlackMessage(slack.Success, "server started")

	return s.Server.StartACMETLSOnly(addr, acmeURL, cacheDir)
}

func (s *Server) SetRemoteHardlinkLocation(path string) {
	s.remoteHardlinkLocation = path
}

func (s *Server) markRequestLogged(rid string) bool {
	if rid == "" {
		return false
	}

	s.requestLogMu.Lock()
	defer s.requestLogMu.Unlock()

	if _, alreadyLogged := s.requestLogged[rid]; alreadyLogged {
		return false
	}

	s.requestLogged[rid] = struct{}{}
	s.requestLogRIDs = append(s.requestLogRIDs, rid)

	if len(s.requestLogRIDs) > maxRememberedRequestLogs {
		oldest := s.requestLogRIDs[0]
		delete(s.requestLogged, oldest)
		s.requestLogRIDs = s.requestLogRIDs[1:]
	}

	return true
}

// EnableJobSubmission enables submission of `ibackup put` jobs to wr in
// response to there being put requests from client backup sets having their
// discovery completed.
//
// Supply the `ibackup put` command (ie. including absolute path to the ibackup
// executable and the option to get put jobs from this server).
//
// Deployment is the wr deployment you wish to use; either 'production' or
// 'development'.
//
// Added jobs will have the given cwd, which matters. If cwd is blank, the
// current working dir is used. If queue is not blank, that queue will be
// forced.
//
// Provide a hint as the the maximum number of put job clients you'll run at
// once, so that reservations can be balanced between them.
func (s *Server) EnableJobSubmission(putCmd, deployment, cwd, queues, queuesAvoid string,
	numClients int, logger log15.Logger) error {
	sched, err := client.New(client.SchedulerSettings{
		Deployment:  deployment,
		Cwd:         cwd,
		Queue:       queues,
		QueuesAvoid: queuesAvoid,
		Timeout:     connectTimeout,
		Logger:      logger,
	})
	if err != nil {
		return err
	}

	s.sched = sched

	req := client.DefaultRequirements()
	req.RAM = reqRAM
	req.Time = reqTime
	s.req = req
	s.putCmd = putCmd

	s.queue.SetReadyAddedCallback(s.rac)
	s.queue.SetTTRCallback(s.ttrc)
	s.numClients = numClients

	return nil
}

// handleRemoveRequests removes objects belonging to the provided reserveGroup
// inside removeQueue from iRODS and data base. This function should be called
// inside a go routine, so the user API request is not locked.
func (s *Server) handleRemoveRequests(sid string) {
	s.discoveryCoordinator.WillRemove(sid)

	for {
		item, removeReq, err := s.reserveRemoveRequest(sid)
		if err != nil {
			break
		}

		s.discoveryCoordinator.WaitForDiscovery(sid)

		err = s.removeRequestFromIRODSandDB(&removeReq)
		if beenReleased := s.handleErrorOrReleaseItem(item, removeReq, err); beenReleased {
			s.discoveryCoordinator.AllowDiscovery(sid)

			continue
		}

		err = s.finalizeRemoveReq(removeReq)
		if err != nil {
			s.Logger.Printf("%s", err)
		}

		s.discoveryCoordinator.AllowDiscovery(sid)
	}

	s.finalizeRemoval(sid)
}

// reserveRemoveRequest reserves an item from the given reserve group from the
// remove queue and converts it to a removeRequest. Returns nil and no error if
// the queue is empty.
func (s *Server) reserveRemoveRequest(reserveGroup string) (*queue.Item, set.RemoveReq, error) {
	item, err := s.removeQueue.Reserve(reserveGroup, retryDelay+2*time.Second)
	if err != nil {
		qerr, ok := err.(queue.Error) //nolint:errorlint
		if ok && errors.Is(qerr.Err, queue.ErrNothingReady) {
			return nil, set.RemoveReq{}, err
		}

		s.Logger.Print(err)
	}

	remReq, err := s.convertQueueItemToRemoveRequest(item.Data())
	if err != nil {
		s.Logger.Print(err)

		return nil, set.RemoveReq{}, err
	}

	return item, remReq, err
}

func (s *Server) convertQueueItemToRemoveRequest(data interface{}) (set.RemoveReq, error) {
	remReq, ok := data.(set.RemoveReq)
	if !ok {
		return set.RemoveReq{}, ErrIncorrectTypeInQueue
	}

	return remReq, nil
}

func (s *Server) removeRequestFromIRODSandDB(removeReq *set.RemoveReq) error {
	if removeReq.IsDir {
		if removeReq.Action == set.ToTrash {
			return s.trashDirFromDB(removeReq.Set, removeReq.Path)
		}

		return s.removeDirFromDB(removeReq.Set.ID(), removeReq.Path)
	}

	return s.removeFileFromIRODSandDB(removeReq)
}

// handleErrorOrReleaseItem returns immediately if there was no error. Otherwise
// it releases the item with updated data from a queue, provided it has attempts
// left, or sets the error on the set. Returns whether the item was released.
func (s *Server) handleErrorOrReleaseItem(item *queue.Item, removeReq set.RemoveReq, err error) bool {
	if err == nil {
		return false
	}

	if item.Stats().Releases >= uint32(jobRetries) {
		errs := s.db.SetError(removeReq.Set.ID(), "Error when removing: "+err.Error())
		if errs != nil {
			s.Logger.Printf("Could not put error on set due to: %s\nError was: %s\n", errs.Error(), err.Error())
		}

		return false
	}

	item.SetData(removeReq)

	err = s.removeQueue.SetDelay(removeReq.Key(), retryDelay)
	if err != nil {
		s.Logger.Printf("%s", err.Error())
	}

	err = s.removeQueue.Release(context.Background(), removeReq.Key())
	if err != nil {
		s.Logger.Printf("%s", err.Error())
	}

	return err == nil
}

func (s *Server) finalizeRemoveReq(removeReq set.RemoveReq) error {
	removeReq.IsComplete = true

	err := s.db.UpdateRemoveRequest(removeReq)
	if err != nil {
		return err
	}

	return s.removeQueue.Remove(context.Background(), removeReq.Key())
}

func (s *Server) finalizeRemoval(sid string) {
	s.discoveryCoordinator.RemovalDone(sid)

	err := s.db.OptimiseRemoveBucket(sid)
	if err != nil {
		s.Logger.Printf("%s", err.Error())
	}

	if s.removeQueue.Stats().Items == 0 {
		s.storageHandler.Cleanup()
	}
}

// rac is our queue's ready added callback which will get all ready put Requests
// and ensure there are enough put jobs added to wr.
//
// We submit up to 100 jobs; with the limit most likely being 10 simulteanous
// jobs at once, that should keep jobs flowing continuously until we next
// trigger the rac.
func (s *Server) rac(_ string, allitemdata []interface{}) {
	n := s.estimateJobsNeeded(len(allitemdata))
	if n == 0 {
		return
	}

	jobs := make([]*jobqueue.Job, n)

	for i := range jobs {
		job := s.sched.NewJob(
			fmt.Sprintf("%s%d", s.putCmd, i),
			repGroup, reqGroup, "", "", s.req,
		)
		job.Retries = jobRetries
		job.LimitGroups = []string{jobLimitGroup}

		jobs[i] = job
	}

	if err := s.sched.SubmitJobs(jobs); err != nil && !strings.Contains(err.Error(), "duplicate") {
		s.Logger.Printf("failed to add jobs to wr's queue: %s", err)
	}

	go func() {
		<-time.After(racRetriggerDelay)
		s.queue.TriggerReadyAddedCallback(context.Background())
	}()
}

// estimateJobsNeeded always returns our numClients, unless the number of
// remaining requests is less than that, in which case it will match that
// number.
func (s *Server) estimateJobsNeeded(numReady int) int {
	if numReady == 0 {
		return 0
	}

	needed := len(s.queue.GetRunningData()) + numReady
	if needed < s.numClients {
		return needed
	}

	return s.numClients
}

// ttrc is called when reserved items in our queue are abandoned due to a put
// client dying, and so we cleanup and send it back to the ready subqueue.
func (s *Server) ttrc(data interface{}) queue.SubQueue {
	r, ok := data.(*transfer.Request)
	if !ok {
		s.Logger.Printf("item data not a Request")
	} else {
		s.uploadTracker.uploadFinished(r)
	}

	return queue.SubQueueReady
}

// clientTTRC is called when clients are assumed to be killed due to not sending
// a heartbeat 5 times consecutively, and removes the iRODS connections.
func (s *Server) clientTTRC(data interface{}) queue.SubQueue {
	hostPID, ok := data.(string)
	if !ok {
		s.Logger.Printf("item data not a hostPID")
	}

	s.sendSlackMessage(slack.Warn, fmt.Sprintf("client host pid %s assumed killed", hostPID))

	s.iRODSTracker.deleteIRODSConnections(hostPID)

	return queue.SubQueueRemoved
}

// stop is called when the server is Stop()ped, cleaning up our additional
// properties.
func (s *Server) stop() {
	s.sendSlackMessage(slack.Warn, "server stopped")
	s.stopHungDebug()

	if s.serverAliveCh != nil {
		close(s.serverAliveCh)
	}

	s.dirPoolMu.Lock()
	s.dirPool.StopWait()
	s.dirPool = nil
	s.dirPoolMu.Unlock()

	if s.statusUpdateCh != nil {
		close(s.statusUpdateCh)
	}

	if s.sched != nil {
		if err := s.sched.Disconnect(); err != nil {
			s.Logger.Printf("scheduler disconnect failed: %s", err)
		}
	}

	if s.db == nil {
		return
	}

	if err := s.db.Close(); err != nil {
		s.Logger.Printf("database close failed: %s", err)
	}

	if err := s.queue.Destroy(); err != nil {
		s.Logger.Printf("queue desrtroy failed: %s", err)
	}

	s.Logger.Printf("gracefully shut down")
}
