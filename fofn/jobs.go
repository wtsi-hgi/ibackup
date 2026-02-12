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
	"path/filepath"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
)

const (
	defaultRAM      = 1024
	defaultCores    = 0.1
	defaultTime     = 8 * time.Hour
	defaultReqGroup = "ibackup"
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

	repGroup := fmt.Sprintf("ibackup_fofn_%s_%d", cfg.SubDirName, cfg.FofnMtime)

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

	if noReplace {
		parts = append(parts, "--no_replace")
	}

	if userMeta != "" {
		parts = append(parts,
			fmt.Sprintf("--meta %q", userMeta))
	}

	parts = append(parts, "-b",
		fmt.Sprintf("-f %q", chunkPath),
		fmt.Sprintf("> %q 2>&1", chunkPath+".out"))

	return strings.Join(parts, " ")
}

func buildPutCoreParts(
	chunkPath, fofnName string,
) []string {
	parts := []string{
		"ibackup put -v",
		fmt.Sprintf("-l %q", chunkPath+".log"),
		fmt.Sprintf("--report %q", chunkPath+".report"),
	}

	if fofnName != "" {
		parts = append(parts,
			fmt.Sprintf("--fofn %q", fofnName))
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
	FindBuriedJobsByRepGroup(prefix string) ([]*jobqueue.Job, error)
	FindIncompleteJobsByRepGroup(prefix string) ([]*jobqueue.Job, error)
	DeleteJobs(jobs []*jobqueue.Job) error
	Disconnect() error
}

// IsRunComplete returns true if there are no incomplete
// jobs for the given repGroup.
func IsRunComplete(
	submitter JobSubmitter, repGroup string,
) (bool, error) {
	jobs, err := submitter.FindIncompleteJobsByRepGroup(repGroup)
	if err != nil {
		return false, err
	}

	return len(jobs) == 0, nil
}

// FindBuriedChunks returns the absolute paths of chunk files extracted from
// buried jobs matching the given repGroup. The chunk path is the argument after
// -f in each job's Cmd, joined with runDir.
func FindBuriedChunks(
	submitter JobSubmitter, repGroup, runDir string,
) ([]string, error) {
	jobs, err := submitter.FindBuriedJobsByRepGroup(repGroup)
	if err != nil {
		return nil, err
	}

	chunks := make([]string, 0, len(jobs))

	for _, job := range jobs {
		chunk := extractChunkFromCmd(job.Cmd)
		if chunk != "" {
			chunks = append(chunks,
				filepath.Join(runDir, chunk))
		}
	}

	return chunks, nil
}

// extractChunkFromCmd parses a command string and returns the argument
// immediately following the -f flag, stripping any surrounding double quotes.
func extractChunkFromCmd(cmd string) string {
	fields := strings.Fields(cmd)

	for i, f := range fields {
		if f == "-f" && i+1 < len(fields) {
			return strings.Trim(fields[i+1], `"`)
		}
	}

	return ""
}

// DeleteBuriedJobs finds and deletes buried jobs matching the given repGroup.
func DeleteBuriedJobs(
	submitter JobSubmitter, repGroup string,
) error {
	jobs, err := submitter.FindBuriedJobsByRepGroup(repGroup)
	if err != nil {
		return err
	}

	if len(jobs) > 0 {
		return submitter.DeleteJobs(jobs)
	}

	return nil
}
