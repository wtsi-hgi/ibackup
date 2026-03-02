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

package fofn

import (
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/inconshreveable/log15"
)

const wrConnectTimeout = 10 * time.Second

type wrSubmitter struct {
	sched *client.Scheduler
}

func (w *wrSubmitter) SubmitJobs(jobs []*jobqueue.Job) error {
	return w.sched.SubmitJobs(jobs)
}

func (w *wrSubmitter) FindJobsByRepGroup(prefix string) ([]*jobqueue.Job, error) {
	return w.sched.FindJobsByRepGroupPrefixAndState(prefix, "")
}

func (w *wrSubmitter) DeleteJobs(jobs []*jobqueue.Job) error {
	return w.sched.RemoveJobs(jobs...)
}

func (w *wrSubmitter) Disconnect() error {
	return w.sched.Disconnect()
}

// NewWRSubmitter connects to wr using the given deployment and returns a
// JobSubmitter backed by the wr scheduler.
func NewWRSubmitter(deployment string, logger log15.Logger) (JobSubmitter, error) { //nolint:ireturn
	sched, err := client.New(client.SchedulerSettings{
		Deployment: deployment,
		Timeout:    wrConnectTimeout,
		Logger:     logger,
	})
	if err != nil {
		return nil, err
	}

	return &wrSubmitter{sched: sched}, nil
}
