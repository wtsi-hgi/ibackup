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
	"fmt"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

// appLogger is used for logging events in our commands.
var appLogger = log15.New()

// global options.
var (
	serverURL  string
	serverCert string
)

const (
	serverURLEnvKey  = "IBACKUP_SERVER_URL"
	serverCertEnvKey = "IBACKUP_SERVER_CERT"
	ConfigKey        = "IBACKUP_CONFIG"
)

const configSubHelp = "Non-prefix transformers can be specified in a config file.\n" +
	"Please check ibackup -h for more information."

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

(You'll need the ` + serverURLEnvKey + ` and ` + serverCertEnvKey + ` environment
variables set up for you by whoever started the server.)

The ` + ConfigKey + ` environmental variable can be set to specify an ibackup
configuration file.

Currently that config file is used to register named transformers. The format of
the file is as follows:

{
	"transformers": {
		"transformer1": {
			"description": "This is my first transformer",
			"re": "^/some/local/path/([^/]+)/files",
			"replace: "/remote/path/files/$1/"
		},
		"transformer2": {
			"description": "This is my second transformer",
			"re": "^/some/other/path/([^/]+)/version_([0-9]+)/",
			"replace: "/remote/path/version_$2/$1/"
		},
		…
	}
}

For manual backups, use the addremote and put sub-commands, eg. to backup
everything in a directory:

find /abs/path/to/dir -type f -print0 | ibackup addremote --humgen -0 -b | ibackup put -b
`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		die(err)
	}
}

func init() {
	// set up logging to stderr
	appLogger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StderrHandler))

	// global flags
	RootCmd.PersistentFlags().StringVar(&serverURL, "url", os.Getenv(serverURLEnvKey),
		"ibackup server URL in the form host:port")
	RootCmd.PersistentFlags().StringVar(&serverCert, "cert", os.Getenv(serverCertEnvKey),
		"path to server certificate file")
}

// ensureURLandCert dies if --url or --cert have not been set.
func ensureURLandCert() {
	if serverURL == "" {
		dief("you must supply --url")
	}

	if serverCert == "" {
		dief("you must supply --cert")
	}
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
