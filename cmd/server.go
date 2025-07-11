/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
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
	"log/syslog"
	"net"
	"os"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/baton"
	"github.com/wtsi-hgi/ibackup/server"
	"github.com/wtsi-hgi/ibackup/set"
	"github.com/wtsi-hgi/ibackup/slack"
	"github.com/wtsi-npg/logshim"
	"github.com/wtsi-npg/logshim-zerolog/zlog"
)

const serverTokenBasename = ".ibackup.token"
const numPutClients = 10
const dbBackupParamPosition = 2
const defaultDebounceSeconds = 600

// options for this cmd.
var serverLogPath string
var serverKey string
var serverLDAPFQDN string
var serverLDAPBindDN string
var serverDebug bool
var readonly bool
var serverRemoteBackupPath string
var serverWRDeployment string
var serverHardlinksCollection string
var serverSlackDebouncePeriod int
var serverStillRunningMsgFreq string

// serverCmd represents the server command.
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the web server",
	Long: `Start the web server.

The ibackup web server is used to record, track, and automate backups of many
backup sets from many users. It can schedule backup jobs to be run in parallel,
and can monitor sets for changes.

If you provide the --hardlinks_collection option, hardlinks will be
de-duplicated by storing them by mountpoint&inode in this iRODS collection.
The original desired location will be an empty file with metadata pointing to
the hardlinks_collection location.

Symlinks will also be stored as empty files, this time with metadata indicating
what the symlink pointed to. The referenced data is NOT backed up.

Starting the web server brings up a web interface and REST API that will use the
given set database path to create a set database if it doesn't exist, add
backup sets to the database, and return information about their status. If you
provide a second database path, the database will be backed up to that path upon
significant changes to the database.

If you also set --remote_backup or the IBACKUP_REMOTE_DB_BACKUP_PATH env var,
and the second database path, the database backup files will also be put in to
iRODS.

Your --url (in this context, think of it as the bind address) should include the
port, and for it to work with your --cert, you probably need to specify it as
fqdn:port. --url defaults to the IBACKUP_SERVER_URL env var. --cert defaults to
the IBACKUP_SERVER_CERT env var.

The server authenticates users using LDAP. You must provide the FQDN for your
LDAP server, eg. --ldap_server ldap.example.com, and the bind DN that you would
supply to eg. 'ldapwhoami -D' to test user credentials, replacing the username
part with '%s', eg. --ldap_dn 'uid=%s,ou=people,dc=example,dc=com'. If you don't
supply both of these, you'll get a warning, but the server will work and assume
all passwords are valid.

The server will log all messages (of any severity) to syslog at the INFO level,
except for non-graceful stops of the server, which are sent at the CRIT level or
include 'panic' in the message. The messages are tagged 'ibackup-server', and
you might want to filter away 'STATUS=200' to find problems.
If --logfile is supplied, logs to that file instead of syslog. It also results
in the put clients we spawn logging to files with that prefix.

To send important events (changes to sets and the server starting and stopping)
to slack for easier monitoring than looking at the logs, set the environment
variables IBACKUP_SLACK_TOKEN and IBACKUP_SLACK_CHANNEL. To get the token you
must first create a Slack application, which needs to be a bot with these
scopes added: chat:write, chat:write.customize, chat:write.public, groups:read
and incoming-webhook, and then add this bot to your workspace. To get the
channel, find the channel ID given after pressing the 'Get channel details'
button (channel title) in the desired channel; it'll be at the bottom of the
pop-up box.

Certain important events, iRODS connections and clients uploading, are
debounced as to not flood the slack channel with messages. This debounce period 
can be set using --slack_debounce, otherwise it will default to 10 minutes 
between each message.

With slack setup, you can also have the server send "still running" messages
periodically by defining the --still_running option.

The server must be running for 'ibackup add' calls to succeed. A wr manager
instance must be running for 'ibackup add' commands to be automatically
scheduled. Set --wr_deployment to "development" if you're using a development
manager.

This command will block forever in the foreground; you can background it with
ctrl-z; bg. Or better yet, use the daemonize program to daemonize this.

If there's an issue with the database or behaviour of the queue, you can use the
--debug option to start the server with job submission disabled on a copy of the
database that you've made, to investigate.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 && len(args) != 2 {
			dief("you must supply the path to your set database file")
		}

		ensureURLandCert()

		if serverKey == "" {
			dief("you must supply --key")
		}

		if serverLDAPFQDN == "" || serverLDAPBindDN == "" {
			warn("ldap options not supplied, will assume all user passwords are correct!")
		}

		logWriter := setServerLogger(serverLogPath)
		token := os.Getenv("IBACKUP_SLACK_TOKEN")
		channel := os.Getenv("IBACKUP_SLACK_CHANNEL")

		var slacker set.Slacker

		if token != "" && channel != "" {
			slacker = slack.New(slack.Config{
				Token:       token,
				Channel:     channel,
				ErrorLogger: logWriter,
			})
		} else {
			if serverStillRunningMsgFreq != "" {
				dief("--still_running requires slack variables")
			}
		}

		var stillRunningMsgFreq time.Duration
		if serverStillRunningMsgFreq != "" {
			var err error

			stillRunningMsgFreq, err = parseDuration(serverStillRunningMsgFreq)
			if err != nil {
				dief("invalid still_running message frequency: %s", err)
			}

			if stillRunningMsgFreq < 1*time.Minute {
				dief("message frequency must be 1m or more, not %s", stillRunningMsgFreq)
			}
		}

		if serverSlackDebouncePeriod < 0 {
			dief("slack_debounce period must be positive, not: %d", serverSlackDebouncePeriod)
		}

		handler, errb := baton.GetBatonHandler()
		if errb != nil {
			dief("failed to get baton handler: %s", errb)
		}

		conf := server.Config{
			HTTPLogger:           logWriter,
			Slacker:              slacker,
			SlackMessageDebounce: time.Duration(serverSlackDebouncePeriod) * time.Second,
			StillRunningMsgFreq:  stillRunningMsgFreq,
			ReadOnly:             readonly,
			StorageHandler:       handler,
		}

		s, err := server.New(conf)
		if err != nil {
			die(err)
		}

		err = s.EnableAuthWithServerToken(serverCert, serverKey, serverTokenBasename, checkPassword)
		if err != nil {
			dief("failed to enable authentication: %s", err)
		}

		err = s.MakeQueueEndPoints()
		if err != nil {
			dief("failed to make queue endpoints: %s", err)
		}

		if serverDebug || readonly {
			warn("job submission has been disabled")
		} else {
			exe, erre := os.Executable()
			if erre != nil {
				dief("failed to get own exe: %s", erre)
			}

			putCmd := fmt.Sprintf("%s put -s --url '%s' --cert '%s' ", exe, serverURL, serverCert)

			if serverLogPath != "" {
				putCmd += fmt.Sprintf("--log %s.client.", serverLogPath)
			}

			err = s.EnableJobSubmission(putCmd, serverWRDeployment, "", "", numPutClients, appLogger)
			if err != nil {
				dief("failed to enable job submission: %s", err)
			}
		}

		info("opening database, please wait...")

		dbBackupPath := ""
		if len(args) == dbBackupParamPosition {
			dbBackupPath = args[dbBackupParamPosition-1]
		}

		if serverHardlinksCollection != "" {
			s.SetRemoteHardlinkLocation(serverHardlinksCollection)
		}

		err = s.LoadSetDB(args[0], dbBackupPath)
		if err != nil {
			dief("failed to load database: %s", err)
		}

		info("loaded database...")

		if serverRemoteBackupPath != "" && !readonly {
			if dbBackupPath == "" {
				dief("remote backup path defined when no local backup path provided")
			}

			info("enabling remote backups...")

			handler, errb := baton.GetBatonHandler()
			if errb != nil {
				dief("failed to get baton handler: %s", errb)
			}

			s.EnableRemoteDBBackups(serverRemoteBackupPath, handler)
			info("enabled remote backups...")
		}

		defer s.Stop()

		sayStarted()

		info("starting server...")
		err = s.Start(serverURL, serverCert, serverKey)
		if err != nil {
			dief("non-graceful stop: %s", err)
		}

		info("graceful server stop")
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)

	// flags specific to this sub-command
	serverCmd.Flags().StringVarP(&serverKey, "key", "k", "",
		"path to key file")
	serverCmd.Flags().StringVarP(&serverLDAPFQDN, "ldap_server", "s", "",
		"fqdn of your ldap server")
	serverCmd.Flags().StringVarP(&serverLDAPBindDN, "ldap_dn", "l", "",
		"ldap bind dn, with username replaced with %s")
	serverCmd.Flags().StringVar(&serverLogPath, "logfile", "",
		"log to this file instead of syslog")
	serverCmd.Flags().StringVarP(&serverWRDeployment, "wr_deployment", "w", "production",
		"use this deployment of wr for your job submission")
	serverCmd.Flags().BoolVar(&serverDebug, "debug", false,
		"disable job submissions for debugging purposes")
	serverCmd.Flags().BoolVar(&readonly, "readonly", false,
		"disable discovery, job submissions, and endpoints that change the database")
	serverCmd.Flags().StringVar(&serverHardlinksCollection, "hardlinks_collection", "",
		"deduplicate hardlinks by storing them by inode in this iRODS collection")
	serverCmd.Flags().StringVar(&serverRemoteBackupPath, "remote_backup", os.Getenv("IBACKUP_REMOTE_DB_BACKUP_PATH"),
		"enables database backup to the specified iRODS path")
	serverCmd.Flags().IntVarP(&serverSlackDebouncePeriod, "slack_debounce", "d", defaultDebounceSeconds,
		"debounced slack messages are sent only once every period of this many seconds"+
			"(eg. 10 for 10 seconds), defaults to 10 minutes")
	serverCmd.Flags().StringVarP(&serverStillRunningMsgFreq, "still_running", "r", "",
		"send a slack message every this period of time to say the server is still running"+
			"(eg. 10m for 10 minutes, or 6h for 6 hours, minimum 1m), defaults to nothing")
}

// setServerLogger makes our appLogger log to the given path if non-blank,
// otherwise to syslog. Returns an io.Writer version of our appLogger for the
// server to log to.
func setServerLogger(path string) io.Writer {
	if path == "" {
		logToSyslog()
	} else {
		logToFile(path)
	}

	lw := &log15Writer{logger: appLogger}

	logshim.InstallLogger(zlog.New(lw, logshim.ErrorLevel))

	return lw
}

// logToSyslog sets our applogger to log to syslog, dies if it can't.
func logToSyslog() {
	fh, err := log15.SyslogHandler(syslog.LOG_INFO|syslog.LOG_DAEMON, "ibackup-server", log15.LogfmtFormat())
	if err != nil {
		dief("failed to log to syslog: %s", err)
	}

	appLogger.SetHandler(fh)
}

// log15Writer wraps a log15.Logger to make it conform to io.Writer interface.
type log15Writer struct {
	logger log15.Logger
}

// Write conforms to the io.Writer interface.
func (w *log15Writer) Write(p []byte) (n int, err error) {
	w.logger.Info(string(p))

	return len(p), nil
}

// checkPassword defers to checkLDAPPassword(). Warns if we don't have the ldap
// details we need, and uses an always true password checker.
func checkPassword(username, password string) (bool, string) {
	if serverLDAPFQDN == "" || serverLDAPBindDN == "" {
		return fakePasswordCheck(username)
	}

	return checkLDAPPassword(username, password)
}

// fakePasswordCheck is for when we don't have ldap credentials, and are just
// testing. It always returns true, unless the username doesn't exist at all.
func fakePasswordCheck(username string) (bool, string) {
	uid, err := gas.UserNameToUID(username)
	if err != nil {
		return false, ""
	}

	return true, uid
}

// checkLDAPPassword checks with LDAP if the given password is valid for the
// given username. Returns true if valid, along with the user's UID.
func checkLDAPPassword(username, password string) (bool, string) {
	uid, err := gas.UserNameToUID(username)
	if err != nil {
		return false, ""
	}

	l, err := ldap.DialURL(fmt.Sprintf("ldaps://%s", net.JoinHostPort(serverLDAPFQDN, "636")))
	if err != nil {
		return false, ""
	}

	err = l.Bind(fmt.Sprintf(serverLDAPBindDN, username), password)
	if err != nil {
		return false, ""
	}

	return true, uid
}

// sayStarted logs to console that the server stated. It does this a second
// after being calling in a goroutine, when we can assume the server has
// actually started; if it failed, we expect it to do so in less than a second
// and exit.
func sayStarted() {
	<-time.After(1 * time.Second)

	info("server started")
}
