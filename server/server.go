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
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gammazero/workerpool"
	"github.com/inconshreveable/log15"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/put"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-ssg/wrstat/v6/scheduler"
)

const (
	ErrNoLogger = gas.Error("a http logger must be configured")

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
	maxJobsToSubmit         = 100
	racRetriggerDelay       = 1 * time.Minute
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
}

// Server is used to start a web server that provides a REST API to the setdb
// package's database, and a website that displays the information nicely.
type Server struct {
	gas.Server
	db                     *set.DB
	numClients             int
	numRequestsCache       []int
	cacheMu                sync.Mutex
	dirPool                *workerpool.WorkerPool
	queue                  *queue.Queue
	removeQueue            *queue.Queue
	sched                  *scheduler.Scheduler
	putCmd                 string
	req                    *jqs.Requirements
	username               string
	remoteHardlinkLocation string
	statusUpdateCh         chan *fileStatusPacket
	monitor                *Monitor
	slacker                set.Slacker
	stillRunningMsgFreq    time.Duration
	serverAliveCh          chan bool
	uploadTracker          *uploadTracker
	readOnly               bool

	mapMu               sync.RWMutex
	creatingCollections map[string]bool
	iRODSTracker        *iRODSTracker
	clientQueue         *queue.Queue
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the required configured io.Writer, which could for example be
// syslog using the log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(conf Config) (*Server, error) {
	if conf.HTTPLogger == nil {
		return nil, ErrNoLogger
	}

	s := &Server{
		Server:              *gas.New(conf.HTTPLogger),
		numClients:          1,
		dirPool:             workerpool.New(workerPoolSizeDir),
		queue:               queue.New(context.Background(), "put"),
		removeQueue:         queue.New(context.Background(), "remove"),
		creatingCollections: make(map[string]bool),
		slacker:             conf.Slacker,
		stillRunningMsgFreq: conf.StillRunningMsgFreq,
		uploadTracker:       newUploadTracker(conf.Slacker, conf.SlackMessageDebounce),
		iRODSTracker:        newiRODSTracker(conf.Slacker, conf.SlackMessageDebounce),
		clientQueue:         queue.New(context.Background(), "client"),
		readOnly:            conf.ReadOnly,
	}

	s.clientQueue.SetTTRCallback(s.clientTTRC)
	s.SetStopCallBack(s.stop)
	s.Server.Router().Use(gas.IncludeAbortErrorsInBody)
	s.removeQueue.SetReadyAddedCallback(s.removeCallback)

	if conf.ReadOnly {
		return s, nil
	}

	s.monitor = NewMonitor(func(given *set.Set) {
		if err := s.discoverSet(given); err != nil {
			s.Logger.Printf("error discovering set during monitoring: %s", err)
		}
	})

	return s, nil
}

// Start logs to slack that the server has been started and then calls
// gas.Server.Start().
func (s *Server) Start(addr, certFile, keyFile string) error {
	s.sendSlackMessage(slack.Success, "server started")

	return s.Server.Start(addr, certFile, keyFile)
}

func (s *Server) SetRemoteHardlinkLocation(path string) {
	s.remoteHardlinkLocation = path
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
func (s *Server) EnableJobSubmission(putCmd, deployment, cwd, queue string, numClients int, logger log15.Logger) error {
	sched, err := scheduler.New(deployment, cwd, queue, "", connectTimeout, logger)
	if err != nil {
		return err
	}

	s.sched = sched

	req := scheduler.DefaultRequirements()
	req.RAM = reqRAM
	req.Time = reqTime
	s.req = req
	s.putCmd = putCmd

	s.queue.SetReadyAddedCallback(s.rac)
	s.queue.SetTTRCallback(s.ttrc)
	s.numClients = numClients

	return nil
}

func (s *Server) removeCallback(queueName string, allitemdata []interface{}) {
	baton, err := put.GetBatonHandlerWithMetaClient()
	if err != nil {
		fmt.Println("error: ", err.Error())
		//
	}

	for _, item := range allitemdata {
		removeReq, _ := item.(removeReq)
		var err error

		if removeReq.isDir {
			err = s.removeDirFromIRODSandDB(removeReq.set, removeReq.path, baton)
		} else {
			err = s.removeFileFromIRODSandDB(removeReq.set, removeReq.path, baton)
		}

		if err != nil {
			fmt.Println("error: ", err.Error())
			//
		}

		err = s.removeQueue.Remove(context.Background(), removeReq.key())
		if err != nil {
			fmt.Println("error: ", err.Error())
			//
		}
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
	r, ok := data.(*put.Request)
	if !ok {
		s.Logger.Printf("item data not a Request")
	}

	s.uploadTracker.uploadFinished(r)

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

	if s.serverAliveCh != nil {
		close(s.serverAliveCh)
	}

	s.dirPool.StopWait()

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
