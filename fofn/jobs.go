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
	"fmt"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/wtsi-hgi/ibackup/internal/shell"
)

const (
	defaultRAM      = 1024
	defaultCores    = 0.1
	defaultTime     = 8 * time.Hour
	defaultReqGroup = "ibackup"

	// RepGroupPrefix is the common prefix for all fofn job repgroups. A single
	// wr query with this prefix retrieves jobs for every fofn directory at
	// once.
	RepGroupPrefix = "ibackup_fofn_"
)

// RunConfig holds configuration for creating jobs from chunk files.
type RunConfig struct {
	RunDir      string
	ChunkPaths  []string
	SubDirName  string
	FofnMtime   int64
	NoReplace   bool
	UserMeta    string
	RAM         int           // MB, default 1024
	Time        time.Duration // default 8h
	Retries     uint8         // set via CLI flag; zero means no retries
	LimitGroups []string      // default []string{"irods"}
	ReqGroup    string        // default "ibackup"
}

// CreateJobs creates jobqueue Jobs from a RunConfig, one per chunk path.
func CreateJobs(cfg RunConfig) []*jobqueue.Job {
	applyDefaults(&cfg)

	repGroup := fmt.Sprintf("%s%s_%d", RepGroupPrefix, cfg.SubDirName, cfg.FofnMtime)

	jobs := make([]*jobqueue.Job, len(cfg.ChunkPaths))

	for i, chunk := range cfg.ChunkPaths {
		jobs[i] = &jobqueue.Job{
			Cmd:        BuildPutCommand(chunk, cfg.NoReplace, cfg.SubDirName, cfg.UserMeta),
			Cwd:        cfg.RunDir,
			CwdMatters: true,
			RepGroup:   repGroup,
			ReqGroup:   cfg.ReqGroup,
			Requirements: &jqs.Requirements{
				RAM:   cfg.RAM,
				Cores: defaultCores,
				Time:  cfg.Time,
			},
			Retries:     cfg.Retries,
			LimitGroups: cfg.LimitGroups,
		}
	}

	return jobs
}

// BuildPutCommand constructs an ibackup put command string
// for a given chunk file. It includes logging, reporting,
// and optional flags for no-replace and user metadata.
func BuildPutCommand(
	chunkPath string,
	noReplace bool,
	fofnName string,
	userMeta string,
) string {
	parts := buildPutCoreParts(chunkPath, fofnName)

	parts = append(parts,
		"-b",
		"-f", shell.Quote(chunkPath),
	)

	if noReplace {
		parts = append(parts, "--no_replace")
	}

	if userMeta != "" {
		parts = append(parts,
			"--meta", shell.Quote(userMeta))
	}

	parts = append(parts,
		">", shell.Quote(chunkPath+".out"), "2>&1")

	return strings.Join(parts, " ")
}

func buildPutCoreParts(
	chunkPath, fofnName string,
) []string {
	parts := []string{
		"ibackup put -v",
		"-l", shell.Quote(chunkPath + ".log"),
		"--report", shell.Quote(chunkPath + ".report"),
	}

	if fofnName != "" {
		parts = append(parts,
			"--fofn", shell.Quote(fofnName))
	}

	return parts
}

func applyDefaults(cfg *RunConfig) {
	if cfg.RAM == 0 {
		cfg.RAM = defaultRAM
	}

	if cfg.Time == 0 {
		cfg.Time = defaultTime
	}

	if cfg.LimitGroups == nil {
		cfg.LimitGroups = []string{"irods"}
	}

	if cfg.ReqGroup == "" {
		cfg.ReqGroup = defaultReqGroup
	}
}

// JobSubmitter is an interface for submitting and querying jobs in a job queue.
type JobSubmitter interface {
	SubmitJobs(jobs []*jobqueue.Job) error
	FindJobsByRepGroup(prefix string) ([]*jobqueue.Job, error)
	DeleteJobs(jobs []*jobqueue.Job) error
	Disconnect() error
}

// RunJobStatus holds the classified result of a wr query for a single repgroup.
type RunJobStatus struct {
	HasRunning   bool
	BuriedJobs   []*jobqueue.Job
	BuriedChunks []string
}

// ClassifyAllJobs queries wr once for all fofn jobs and returns a map keyed by
// repgroup with classified status.
func ClassifyAllJobs(submitter JobSubmitter) (map[string]RunJobStatus, error) {
	jobs, err := submitter.FindJobsByRepGroup(
		RepGroupPrefix,
	)
	if err != nil {
		return nil, err
	}

	return classifyJobs(jobs), nil
}

func classifyJobs(jobs []*jobqueue.Job) map[string]RunJobStatus {
	result := make(map[string]RunJobStatus)

	for _, job := range jobs {
		rg := job.RepGroup
		s := result[rg]

		switch job.State {
		case jobqueue.JobStateComplete, jobqueue.JobStateDeleted:
			continue
		case jobqueue.JobStateBuried:
			s.BuriedJobs = append(s.BuriedJobs, job)

			chunk := extractChunkFromCmd(job.Cmd)
			if chunk != "" {
				s.BuriedChunks = append(s.BuriedChunks, chunk)
			}
		default:
			s.HasRunning = true
		}

		result[rg] = s
	}

	return result
}

// extractChunkFromCmd parses a command string and returns the argument
// immediately following the -f flag, stripping any surrounding double quotes.
// The argument may be quoted (e.g. -f "path/to/chunk") to support paths
// containing spaces.
func extractChunkFromCmd(cmd string) string {
	const flag = "-f "

	idx := strings.Index(cmd, flag)
	if idx == -1 {
		return ""
	}

	rest := cmd[idx+len(flag):]

	return extractQuotedOrWord(rest)
}

func extractQuotedOrWord(s string) string {
	if len(s) == 0 {
		return ""
	}

	if s[0] == '"' || s[0] == '\'' {
		if end := strings.IndexByte(s[1:], s[0]); end >= 0 {
			return s[1 : end+1]
		}

		return s[1:]
	}

	if end := strings.IndexByte(s, ' '); end >= 0 {
		return s[:end]
	}

	return s
}
