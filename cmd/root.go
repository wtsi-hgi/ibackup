/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

// package cmd is the cobra file that enables subcommands and handles
// command-line args.

package cmd

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

// appLogger is used for logging events in our commands.
var appLogger = log15.New()

var exitCode int

// global options.
var (
	serverURL  = os.Getenv(serverURLEnvKey)
	serverCert = os.Getenv(serverCertEnvKey)
)

var (
	ErrEmptyFlag     = errors.New("flag cannot be empty")
	ErrInvalidOption = errors.New("invalid option")
)

const (
	serverURLEnvKey  = "IBACKUP_SERVER_URL"
	serverCertEnvKey = "IBACKUP_SERVER_CERT"
)

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "ibackup",
	Short: "ibackup backs up local files to iRODS",
	Long: `ibackup backs up local files to iRODS.

For automated backups via an ibackup server, use the add and status
sub-commands, eg.:

echo /abs/path/to/file1.txt > files.txt
echo /abs/path/to/file2.txt >> files.txt
echo /abs/path/to/dir1 > dirs.txt
echo /abs/path/to/dir2 >> dirs.txt
ibackup add -n myfirstbackup -t 'humgen' -f files.txt -d dirs.txt
ibackup status

(You'll need the IBACKUP_SERVER_URL and IBACKUP_SERVER_CERT environment
variables set up for you by whoever started the server.)


For manual backups, use the addremote and put sub-commands, eg. to backup
everything in a directory:

find /abs/path/to/dir -type f -print0 | ibackup addremote --humgen -0 -b | ibackup put -b
`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the RootCmd.
func Execute() int {
	if err := RootCmd.Execute(); err != nil {
		appLogger.Error(err.Error())

		exitCode = 1
	}

	ec := exitCode

	exitCode = 0

	return ec
}

func must(err error) {
	if err != nil {
		die(err)
	}
}

type enumFlag struct {
	options []string
	value   string
}

func newEnumFlag(options []string, def string) *enumFlag {
	return &enumFlag{options, def}
}

func (e *enumFlag) Set(val string) error {
	if !slices.Contains(e.options, val) {
		return ErrInvalidOption
	}

	e.value = val

	return nil
}
func (*enumFlag) Type() string {
	return "string"
}

func (e *enumFlag) String() string {
	return e.value
}

type stringFlag struct {
	str *string
}

func (s *stringFlag) Set(val string) error {
	if val == "" {
		return ErrEmptyFlag
	}

	*s.str = val

	return nil
}
func (*stringFlag) Type() string {
	return "string"
}

func (s *stringFlag) String() string {
	return *s.str
}

func init() {
	// set up logging to stderr
	appLogger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StderrHandler))

	// global flags
	RootCmd.PersistentFlags().Var(&stringFlag{&serverURL}, "url",
		"ibackup server URL in the form host:port")
	RootCmd.PersistentFlags().Var(&stringFlag{&serverCert}, "cert",
		"path to server certificate file")

	must(RootCmd.MarkPersistentFlagRequired("url"))
	must(RootCmd.MarkPersistentFlagRequired("cert"))
}

// logToFile logs to the given file.
func logToFile(path string) {
	fh, err := log15.FileHandler(path, log15.LogfmtFormat())
	if err != nil {
		fh = log15.StderrHandler

		warn("can't write to log file; logging to stderr instead (%s)", err)
	}

	appLogger.SetHandler(fh)
}

func cliPrint(msg string) {
	fmt.Fprint(os.Stdout, msg)
}

// cliPrintf outputs the message to STDOUT.
func cliPrintf(msg string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, a...)
}

// cliPrintRaw is like cliPrint, but does no interpretation of placeholders in
// msg.
func cliPrintRaw(msg string) {
	fmt.Fprint(os.Stdout, msg)
}

// info is a convenience to log a message at the Info level.
func info(msg string, a ...interface{}) {
	appLogger.Info(fmt.Sprintf(msg, a...))
}

// warn is a convenience to log a message at the Warn level.
func warn(msg string, a ...interface{}) {
	appLogger.Warn(fmt.Sprintf(msg, a...))
}

// die is a convenience to log a message at the Error level and exit non zero.
func die(err error) {
	appLogger.Error(err.Error())
	os.Exit(1)
}

// die is a convenience to log a message at the Error level and exit non zero.
func dief(msg string, a ...interface{}) {
	appLogger.Error(fmt.Sprintf(msg, a...))
	os.Exit(1)
}
