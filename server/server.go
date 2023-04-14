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
	"os/user"
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
	"github.com/wtsi-ssg/wrstat/v3/scheduler"
)

const (
	// workerPoolSizeFiles is the max number of concurrent file stats we'll do
	// during discovery.
	workerPoolSizeFiles = 16

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

// Server is used to start a web server that provides a REST API to the setdb
// package's database, and a website that displays the information nicely.
type Server struct {
	gas.Server
	db                  *set.DB
	numClients          int
	numRequestsCache    []int
	cacheMu             sync.Mutex
	filePool            *workerpool.WorkerPool
	dirPool             *workerpool.WorkerPool
	queue               *queue.Queue
	sched               *scheduler.Scheduler
	putCmd              string
	req                 *jqs.Requirements
	username            string
	statusUpdateCh      chan *fileStatusPacket
	creatingCollections map[string]bool
	uploading           map[string]*put.Request
	stuckRequests       map[string]*put.Request
	mapMu               sync.RWMutex
	monitor             *Monitor
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the given io.Writer, which could for example be syslog using the
// log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(logWriter io.Writer) *Server {
	s := &Server{
		Server:              *gas.New(logWriter),
		numClients:          1,
		filePool:            workerpool.New(workerPoolSizeFiles),
		dirPool:             workerpool.New(workerPoolSizeDir),
		queue:               queue.New(context.Background(), "put"),
		creatingCollections: make(map[string]bool),
		uploading:           make(map[string]*put.Request),
		stuckRequests:       make(map[string]*put.Request),
	}

	s.monitor = NewMonitor(func(given *set.Set) {
		if err := s.discoverSet(given); err != nil {
			s.Logger.Printf("error discovering set during monitoring: %s", err)
		}
	})

	s.SetStopCallBack(s.stop)

	return s
}

// EnableAuth does the same as gas.EnableAuth, but also records the current
// username as a user with root-like permissions to work with everyone's
// backup sets.
func (s *Server) EnableAuth(certFile, keyFile string, acb gas.AuthCallback) error {
	u, err := user.Current()
	if err != nil {
		return err
	}

	s.username = u.Username

	return s.Server.EnableAuth(certFile, keyFile, acb)
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
	sched, err := scheduler.New(deployment, cwd, queue, connectTimeout, logger, false)
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

// rac is our queue's ready added callback which will get all ready put Requests
// and ensure there are enough put jobs added to wr.
//
// We submit up to 100 jobs; with the limit most likely being 10 simulteanous
// jobs at once, that should keep jobs flowing continuously until we next
// trigger the rac.
func (s *Server) rac(queuename string, allitemdata []interface{}) {
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
	s.mapMu.Lock()
	defer s.mapMu.Unlock()

	r, ok := data.(*put.Request)
	if !ok {
		s.Logger.Printf("item data not a Request")
	}

	rid := r.ID()
	delete(s.uploading, rid)
	delete(s.stuckRequests, rid)

	return queue.SubQueueReady
}

// stop is called when the server is Stop()ped, cleaning up our additional
// properties.
func (s *Server) stop() {
	s.filePool.StopWait()
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
