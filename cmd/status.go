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

package cmd

import (
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/transformer"
)

const dateShort = "06/01/02"
const bytesInMiB = 1024 * 1024
const hundredForPercentCalc float64 = 100
const nsInWeek = hoursInWeek * time.Hour
const nsInDay = hoursInDay * time.Hour
const alphabetic = "alphabetic"
const recent = "recent"

// options for this cmd.
var statusUser string
var statusOrder string
var statusName string
var statusDetails bool
var statusRemotePaths bool
var statusIncomplete bool
var statusComplete bool
var statusFailed bool
var statusQueued bool
var statusShowHidden bool
var statusTrash bool

// statusCmd represents the status command.
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of your backup sets",
	Long: `Get the status of your backup sets.

Having used 'ibackup add' to add the details of one or more backup sets, use
this command to get the current backup status of your sets. Provide --name to
get the status of just that set, and --details to get the individual backup
status of every file in the set (only possible with a --name). Provide 
--remotepaths alongside --details to also display the remote path for each file
(only possible with --details and a --name).

When not using --name, provide one of:
--incomplete to only see currently incomplete sets. This will include sets
  with failures but everything else uploaded (shown with a "complete" status,
  see below), but exclude sets where everything is missing or uploaded.
--complete to only see sets that both have a "complete" status and no failures.
--failed to only see sets that have failed uploads or fundamental failures with
  the definition of the set.
--queued to only see sets that have been added and are queued, but have not yet
  started to upload any files.

If none of the above flags are supplied, you'll get the status of all your sets.

Provide --order with either 'alphabetic' (default) or 'recent' to determine the 
order the sets are displayed. Choosing recent puts the most recently discovered
sets towards the end of the list, with undiscovered sets last.

You need to supply the ibackup server's URL in the form domain:port (using the
IBACKUP_SERVER_URL environment variable, or overriding that with the --url
argument) and if necessary, the certificate (using the IBACKUP_SERVER_CERT
environment variable, or overriding that with the --cert argument).

Once a backup set has been added, it goes through these statuses:
pending discovery: the server will check which files in the set exist, and
  discover the contents of directories in the set.
pending upload: the server completed discovery, and has queued your files to be
  backed up, but none have been backed up yet. Most likely because other files
  from other sets are being backed up first.
uploading: at least one file in your set has been uploaded to iRODS, but not all
  of them have.
failing: at least one file in your set has failed to upload to iRODS after
  multiple retries, and the server has given up on it while it continues to try
  to upload other files in your set.
complete: all files in your backup set were either missing, successfully
  uploaded, or failed.

With --details, you'll see tab-separated columns of Path, Status, Size, Date
and Error, with one file per line. If you include --remotepaths you will also 
see a column for Remote Path.

Without --details, you'll still see these details for files that failed their
upload.

If you are the user who started the ibackup server, you can use the --user
option to get the status of a given requestor's backup sets, instead of your
own. You can specify the user as "all" to see all user's sets.
`,
	Run: func(cmd *cobra.Command, args []string) {
		ensureURLandCert()

		if statusDetails && statusName == "" {
			dief("--details can only be used with --name")
		}

		if statusRemotePaths && !statusDetails {
			dief("--remote can only be used with --details and --name")
		}

		if statusOrder != alphabetic && statusOrder != recent {
			dief("--order can only be 'alphabetic' or 'recent'")
		}

		sf := newStatusFilterer(statusIncomplete, statusComplete, statusFailed, statusQueued, statusTrash)

		if statusName != "" && sf != nil && !statusTrash {
			dief("--name can't be used together with the status filtering options")
		}

		client, err := newServerClient(serverURL, serverCert)
		if err != nil {
			die(err)
		}

		status(client, sf, statusUser, statusOrder, statusName, statusDetails,
			statusRemotePaths, statusShowHidden, statusTrash)
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)

	// flags specific to this sub-command
	statusCmd.Flags().StringVar(&statusUser, "user", currentUsername(), helpTextuser)
	statusCmd.Flags().StringVarP(&statusOrder, "order", "o", alphabetic,
		"show sets in 'alphabetic' or 'recent' order")
	statusCmd.Flags().StringVarP(&statusName, "name", "n", "",
		"get status for just the set with this name")
	statusCmd.Flags().BoolVarP(&statusDetails, "details", "d", false,
		"in combination with --name, show the status of every file in the set")
	statusCmd.Flags().BoolVarP(&statusRemotePaths, "remotepaths", "r", false,
		"in combination with --name and --details, show the remote path of every file in the set")
	statusCmd.Flags().BoolVarP(&statusIncomplete, "incomplete", "i", false,
		"only show currently incomplete sets")
	statusCmd.Flags().BoolVarP(&statusComplete, "complete", "c", false,
		"only show truly complete sets (no failures)")
	statusCmd.Flags().BoolVarP(&statusFailed, "failed", "f", false,
		"only show sets with failed uploads")
	statusCmd.Flags().BoolVarP(&statusQueued, "queued", "q", false,
		"only show queued sets (added but hasn't started to upload yet)")
	statusCmd.Flags().BoolVar(&statusShowHidden, "show-hidden", false,
		"show hidden sets")
	statusCmd.Flags().BoolVar(&statusTrash, "trash", false,
		"show trash for a single set or for all sets")

	if !isAdmin() {
		if err := statusCmd.Flags().MarkHidden("user"); err != nil {
			die(err)
		}
		if err := statusCmd.Flags().MarkHidden("trash"); err != nil {
			die(err)
		}
	}
}

type statusFilterer func(*set.Set) bool

func newStatusFilterer(incomplete, complete, failed, queued, trashed bool) statusFilterer {
	checkOnlyOneStatusFlagSet(incomplete, complete, failed, queued, trashed)

	switch {
	case incomplete:
		return func(given *set.Set) bool {
			return given.Incomplete()
		}
	case complete:
		return func(given *set.Set) bool {
			return !given.Incomplete()
		}
	case failed:
		return func(given *set.Set) bool {
			return given.HasProblems()
		}
	case queued:
		return func(given *set.Set) bool {
			return given.Queued()
		}
	case trashed:
		return func(given *set.Set) bool {
			return given.IsTrash()
		}
	default:
		return nil
	}
}

func checkOnlyOneStatusFlagSet(incomplete, complete, failed, queued, trashed bool) {
	flagSeen := false

	for _, flag := range []bool{incomplete, complete, failed, queued, trashed} {
		if flag {
			if flagSeen {
				dief("--incomplete, --complete, --failed, --queued and --trashed are mutually exclusive")
			}

			flagSeen = true
		}
	}
}

// status does the main job of getting backup set status from the server.
func status(client *server.Client, sf statusFilterer, user, order, name string,
	details, remote, showHidden, trash bool) {
	qs, err := client.GetQueueStatus()
	if err != nil {
		dief("unable to get server queue status: %s", err)
	}

	displayQueueStatus(qs)

	var sets []*set.Set

	if name != "" {
		if trash {
			name = set.TrashPrefix + name
		}

		sets = append(sets, getSetByName(client, user, name))
	} else {
		sets = getSets(client, sf, user, showHidden, trash)
	}

	if len(sets) == 0 {
		warn("no backup sets")

		return
	}

	displaySets(client, sets, details, remote, user == "all", order)
}

// displayQueueStatus prints out QStatus in a nice way. If user is admin, also
// shows details of stuck requests.
func displayQueueStatus(qs *server.QStatus) {
	info("Global put queue status: %d queued; %d reserved to be worked on; %d failed",
		qs.Total, qs.Reserved, qs.Failed)
	info("Global put client status (/%d): %d iRODS connections; %d creating collections; %d currently uploading",
		numPutClients, qs.IRODSConnections, qs.CreatingCollections, qs.Uploading)

	if qs.Stuck != nil {
		if gasClientCLI(serverURL, serverCert).CanReadServerToken() {
			for _, r := range qs.Stuck {
				warn("set '%s' for %s [%s => %s] %s", r.Set, r.Requester, r.Local, r.Remote, r.Stuck)
			}
		}
	}
}

// getSetByName gets a set with the given name owned by the given user. Dies
// on error.
func getSetByName(client *server.Client, user, name string) *set.Set {
	got, err := client.GetSetByName(user, name)
	if err != nil {
		dief("%s [%s]", err, name)
	}

	return got
}

// getSets gets all or filtered sets belonging to the given user. Dies on error.
func getSets(client *server.Client, sf statusFilterer, user string, showHidden, showTrash bool) []*set.Set {
	sets, err := client.GetSets(user)
	if err != nil {
		die(err)
	}

	if !showHidden {
		sets = filterHidden(sets)
	}

	if !showTrash {
		sets = filter(func(s *set.Set) bool {
			return !s.IsTrash()
		}, sets)
	}

	if sf != nil {
		sets = filter(sf, sets)
	}

	return sets
}

// filter returns the desired subset from amongst the given sets.
//
//   - "incomplete" includes sets with status complete, but failures.
//   - "complete" is sets with status complete, but excluding those with
//     failures or errors.
//   - "failed" is any set with any failures or errors.
//   - "queued" is sets that are between "only just added" and their first
//     upload.
func filter(sf statusFilterer, sets []*set.Set) []*set.Set {
	var filtered []*set.Set

	for _, s := range sets {
		if sf(s) {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

func filterHidden(sets []*set.Set) []*set.Set {
	filtered := make([]*set.Set, 0, len(sets))

	for _, s := range sets {
		if s.Hide {
			continue
		}

		filtered = append(filtered, s)
	}

	return filtered
}

// displaySets prints info about the given sets to STDOUT. Failed entry details
// will also be printed, and optionally non-failed.
func displaySets(client *server.Client, sets []*set.Set,
	showNonFailedEntries, showRemotePaths, showRequesters bool, order string) {
	l := len(sets)

	sortSets(order, sets)

	for i, forDisplay := range sets {
		cliPrint("\n")

		displaySet(forDisplay, showRequesters)

		transformer := getSetTransformer(forDisplay)

		displayDirs(getDirs(client, forDisplay.ID()), transformer)
		displayExampleFile(getExampleFile(client, forDisplay.ID()), transformer)

		if showNonFailedEntries {
			displayAllEntries(client, forDisplay, showRemotePaths, transformer)
		} else {
			displayFailedEntries(client, forDisplay)
		}

		if i != l-1 {
			cliPrint("\n-----\n")
		}
	}
}

// sortSets sorts the slice of sets alphabetically by default or by most
// recently discovered files.
func sortSets(order string, sets []*set.Set) {
	if order == recent {
		sortSetsByRecent(sets)

		return
	}

	sortSetsAlphabetically(sets)
}

// sortSetsByRecent sorts the sets by the most recently discovered being towards
// the end, with undiscovered files being put last.
func sortSetsByRecent(sets []*set.Set) {
	sort.Slice(sets, func(i, j int) bool {
		if sets[i].LastDiscovery.IsZero() && !sets[j].LastDiscovery.IsZero() {
			return false
		}

		if !sets[i].LastDiscovery.IsZero() && sets[j].LastDiscovery.IsZero() {
			return true
		}

		return sets[i].LastDiscovery.After(sets[j].LastDiscovery)
	})
}

func sortSetsAlphabetically(sets []*set.Set) {
	sort.Slice(sets, func(i, j int) bool {
		return sets[i].Name < sets[j].Name
	})
}

// displaySet prints info about the given set to STDOUT.
func displaySet(s *set.Set, showRequesters bool) { //nolint:funlen,gocyclo,gocognit,cyclop
	cliPrintf("Name: %s\n", s.Name)

	if showRequesters {
		cliPrintf("Requester: %s\n", s.Requester)
	}

	if s.ReadOnly {
		cliPrintf("Read-only: true\n")
	}

	cliPrintf("Transformer: %s\n", s.Transformer)

	cliPrintf("Reason: %s\n", s.Metadata["ibackup:reason"])
	cliPrintf("Review date: %.10s\n", s.Metadata["ibackup:review"])
	cliPrintf("Removal date: %.10s\n", s.Metadata["ibackup:removal"])

	userMeta := s.UserMetadata()
	if userMeta != "" {
		cliPrintf("User metadata: %s\n", userMeta)
	}

	monitored := "false"
	if s.MonitorTime > 0 {
		monitored = formatDuration(s.MonitorTime)
	}

	monitorStr := "Monitored"
	if s.MonitorRemovals {
		monitorStr += " (with removals)"
	}

	cliPrintf("%s: %s; Archive: %v\n", monitorStr, monitored, s.DeleteLocal)

	if s.Description != "" {
		cliPrintf("Description: %s\n", s.Description)
	}

	if s.Error != "" {
		cliPrintf("Status: unable to proceed\n")
		cliPrintf("Error: %s\n", s.Error)
	} else if s.Status == set.Complete && s.Failed != 0 {
		cliPrintf("Status: %s (but with failures - try a retry)\n", s.Status)
	} else {
		cliPrintf("Status: %s\n", s.Status)
	}

	if s.NumObjectsToBeRemoved > 0 {
		cliPrintf("Removal status: %d / %d objects removed\n", s.NumObjectsRemoved, s.NumObjectsToBeRemoved)
	}

	if s.Warning != "" {
		cliPrintf("Warning: %s\n", s.Warning)
	}

	cliPrintf("Discovery: %s\n", s.Discovered())
	cliPrintf("Num files: %s; Symlinks: %d; Hardlinks: %d; Size (total/recently uploaded/recently removed): %s / %s / %s\n", //nolint:lll
		s.Count(), s.Symlinks, s.Hardlinks, s.Size(), s.UploadedSize(), s.RemovedSize())
	cliPrintf("Uploaded: %d; Replaced: %d; Skipped: %d; Failed: %d; Missing: %d; Orphaned: %d; Abnormal: %d\n",
		s.Uploaded, s.Replaced, s.Skipped, s.Failed, s.Missing, s.Orphaned, s.Abnormal)

	switch s.Status {
	case set.Complete:
		cliPrintf("Completed in: %s\n", s.LastCompleted.Sub(s.StartedDiscovery).Truncate(time.Second))
	case set.Uploading:
		displayETA(s)
	default:
	}
}

func formatDuration(dur time.Duration) string {
	var sb strings.Builder

	dur = formatDurationPart(&sb, dur, nsInWeek, "w")
	dur = formatDurationPart(&sb, dur, nsInDay, "d")
	dur = formatDurationPart(&sb, dur, time.Hour, "h")
	dur = formatDurationPart(&sb, dur, time.Minute, "m")
	formatDurationPart(&sb, dur, time.Second, "s")

	return sb.String()
}

func formatDurationPart(w io.Writer, dur, partDur time.Duration, unit string) time.Duration {
	if dur >= partDur {
		fmt.Fprintf(w, "%d%s", dur/partDur, unit)

		dur %= partDur
	}

	return dur
}

// displayETA prints info about ETA for the given currently uploading set to
// STDOUT.
func displayETA(s *set.Set) {
	var (
		basedOn   string
		total     uint64
		done      uint64
		remaining float64
		speed     float64
		unit      string
		timeUnit  time.Duration
	)

	if s.LastCompletedSize > 0 {
		basedOn, unit, total, done, remaining, speed, timeUnit = determineETADetailsFromSize(s)
	} else {
		basedOn, unit, total, done, remaining, speed, timeUnit = determineETADetailsFromCount(s)
	}

	if done == 0 {
		return
	}

	percentComplete := (hundredForPercentCalc / float64(total)) * float64(done)
	eta := time.Duration((remaining / speed) * float64(timeUnit))

	cliPrintf("%.2f%% complete (based on %s); %.2f %s; ETA: %s\n",
		percentComplete, basedOn, speed, unit, eta.Round(time.Second))
}

func determineETADetailsFromSize(s *set.Set) (basedOn, unit string, total, done uint64, //nolint:unparam
	remaining, speed float64, timeUnit time.Duration) {
	basedOn = "last completed size"
	total = s.LastCompletedSize
	done = s.SizeTotal
	remaining = bytesToMB(total - done)

	if done == 0 {
		return
	}

	speed = bytesToMB(done) / time.Since(s.LastDiscovery).Seconds()
	unit = "MB/s"
	timeUnit = time.Second

	return
}

func determineETADetailsFromCount(s *set.Set) (basedOn, unit string, total, done uint64, //nolint:unparam
	remaining, speed float64, timeUnit time.Duration) {
	basedOn = "number of files"
	total = s.NumFiles
	done = s.Uploaded
	remaining = float64(total - done)

	if done == 0 {
		return
	}

	speed = float64(done) / time.Since(s.LastDiscovery).Hours()
	unit = "files/hr"
	timeUnit = time.Hour

	return
}

// bytesToMB converts bytes to number of MB.
func bytesToMB(bytes uint64) float64 {
	return float64(bytes) / bytesInMiB
}

// getSetTransformer returns the set's tranformer, or dies.
func getSetTransformer(given *set.Set) transformer.PathTransformer {
	transformer, err := given.MakeTransformer()
	if err != nil {
		dief("your transformer didn't work: %s", err)
	}

	return transformer
}

// getDirs gets the dir entries for a set and returns their paths along with
// their status.
func getDirs(client *server.Client, setID string) map[string]set.EntryStatus {
	got, err := client.GetDirs(setID)
	if err != nil {
		die(err)
	}

	paths := make(map[string]set.EntryStatus, len(got))

	for _, entry := range got {
		paths[entry.Path] = entry.Status
	}

	return paths
}

// displayDirs prints out directories one per line with a header, if dirs is not
// empty.
func displayDirs(dirs map[string]set.EntryStatus, transformer transformer.PathTransformer) {
	if len(dirs) == 0 {
		return
	}

	cliPrint("Directories:\n")

	warnedAboutTransformer := false

	for dir, dirStatus := range dirs {
		statusStr, remoteStatusStr := generateDirStatusStrings(dirStatus)

		pretendFile := filepath.Join(dir, "file.txt")
		transformedPath, err := transformer(pretendFile)
		if err != nil {
			if !warnedAboutTransformer {
				warn("your transformer didn't work: %s", err)

				warnedAboutTransformer = true
			}

			cliPrintf("  %s%s\n", dir, statusStr)

			continue
		}

		cliPrintf("  %s%s => %s%s\n", dir, statusStr, filepath.Dir(transformedPath), remoteStatusStr)
	}
}

func generateDirStatusStrings(dirStatus set.EntryStatus) (string, string) {
	statusStr := ""
	remoteStatusStr := ""

	if dirStatus == set.Missing || dirStatus == set.Orphaned {
		statusStr = " (missing)"
		remoteStatusStr = fmt.Sprintf(" (%s)", dirStatus.String())
	}

	return statusStr, remoteStatusStr
}

// getExampleFile gets an example file entry for a set and returns its path.
func getExampleFile(client *server.Client, setID string) string {
	exampleFile, err := client.GetExampleFile(setID)
	if err != nil {
		die(err)
	}

	if exampleFile == nil {
		return ""
	}

	return exampleFile.Path
}

func displayExampleFile(path string, transformer transformer.PathTransformer) {
	if path == "" {
		return
	}

	transformedPath, err := transformer(path)
	if err != nil {
		warn("your transformer didn't work: %s", err)

		return
	}

	cliPrintf("Example File: %s => %s\n", path, transformedPath)
}

// displayFailedEntries prints out details about up to 10 failed entries in the
// given set.
func displayFailedEntries(client *server.Client, given *set.Set) {
	failed, skipped, err := client.GetFailedFiles(given.ID())
	if err != nil {
		die(err)
	}

	displayEntries(failed, false, false, nil)

	if skipped > 0 {
		cliPrintf("[... and %d others]\n", skipped)
	}
}

// displayAllEntries prints out details about all entries in the given
// set.
func displayAllEntries(client *server.Client, given *set.Set, showRemotePaths bool,
	transformer transformer.PathTransformer) {
	all, err := client.GetFiles(given.ID())
	if err != nil {
		die(err)
	}

	showTrashDate := strings.HasPrefix(given.Name, set.TrashPrefix)

	displayEntries(all, showRemotePaths, showTrashDate, transformer)
}

// displayEntries prints info about the given file entries to STDOUT.
func displayEntries(entries []*set.Entry, showRemotePaths,
	showTrashDate bool, transformer transformer.PathTransformer) {
	if len(entries) == 0 {
		return
	}

	displayHeader(showRemotePaths, showTrashDate)

	for _, entry := range entries {
		remotePath := getRemotePath(entry.Path, transformer, showRemotePaths)
		displayEntry(entry, showTrashDate, remotePath)
	}
}

// displayHeader adds a column for remote path if showRemotePath is true and
// prints the header.
func displayHeader(showRemotePath, showTrashDate bool) {
	cols := []string{"Local Path", "Status", "Size", "Attempts", "Date", "Error"}

	if showTrashDate {
		cols = slices.Insert(cols, 1, "Trash Date")
	}

	if showRemotePath {
		cols = slices.Insert(cols, 1, "Remote Path")
	}

	printEntriesHeader(cols)
}

// printEntriesHeader prints a header including the given columns relating to
// the output of entry details.
func printEntriesHeader(cols []string) {
	cliPrint("\n")
	cliPrintf("%s", strings.Join(cols, "\t"))
	cliPrint("\n")
}

// getRemotePath returns the remote path for a given path.
func getRemotePath(path string, transformer transformer.PathTransformer, wantRemote bool) string {
	if !wantRemote {
		return ""
	}

	remotePath, err := transformer(path)
	if err != nil {
		dief("your transformer didn't work: %s", err)
	}

	return remotePath
}

// displayEntry displays information about a given entry, including its remote path
// if it's set.
func displayEntry(entry *set.Entry, showTrashDate bool, remotePath string) {
	var date string

	if entry.LastAttempt.IsZero() {
		date = "-"
	} else {
		date = entry.LastAttempt.Format(dateShort)
	}

	cols := []string{
		entry.Path,
		entry.Status.String(),
		humanize.IBytes(entry.Size), //nolint:misspell
		strconv.Itoa(entry.Attempts),
		date,
		entry.LastError,
	}

	if showTrashDate {
		cols = slices.Insert(cols, 1, entry.TrashDate.Format(dateShort))
	}

	if remotePath != "" {
		cols = slices.Insert(cols, 1, remotePath)
	}

	cliPrintRaw(strings.Join(cols, "\t"))
	cliPrintRaw("\n")
}
