/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"os/user"
	"path/filepath"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	gas "github.com/wtsi-hgi/go-authserver"
	"github.com/wtsi-hgi/ibackup/server"
)

const serverTokenBasename = ".ibackup.token"

var serverUser string
var serverUID string
var serverToken []byte

// options for this cmd.
var serverLogPath string
var serverKey string
var serverLDAPFQDN string
var serverLDAPBindDN string

// serverCmd represents the server command.
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the web server",
	Long: `Start the web server.

Starting the web server brings up a web interface and REST API that will use the
given set database path to create a set database if it doesn't exist, add
backup sets to the database, and return information about their status.

Your --url (in this context, think of it as the bind address) should include the
port, and for it to work with your --cert, you probably need to specify it as
fqdn:port. --url defaults to the IBACKUP_SERVER_URL env var. --cert defaults to
the IBACKUP_SERVER_CERT env var.

The server authenticates users using LDAP. You must provide the FQDN for your
LDAP server, eg. --ldap_server ldap.example.com, and the bind DN that you would
supply to eg. 'ldapwhoami -D' to test user credentials, replacing the username
part with '%s', eg. --ldap_dn 'uid=%s,ou=people,dc=example,dc=com'.

The server will log all messages (of any severity) to syslog at the INFO level,
except for non-graceful stops of the server, which are sent at the CRIT level or
include 'panic' in the message. The messages are tagged 'ibackup-server', and
you might want to filter away 'STATUS=200' to find problems.
If --logfile is supplied, logs to that file instaed of syslog.

The server must be running for 'ibackup add' calls to succeed.

This command will block forever in the foreground; you can background it with
ctrl-z; bg. Or better yet, use the daemonize program to daemonize this.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to your set database file")
		}

		ensureURLandCert()

		if serverKey == "" {
			die("you must supply --key")
		}

		if serverLDAPFQDN == "" {
			die("you must supply --ldap_server")
		}

		if serverLDAPBindDN == "" {
			die("you must supply --ldap_dn")
		}

		logWriter := setServerLogger(serverLogPath)

		s := server.New(logWriter)

		prepareAuth(s)

		exe, err := os.Executable()
		if err != nil {
			die("failed to get own exe: %s", err)
		}

		err = s.EnableJobSubmission(fmt.Sprintf("%s put -s -u '%s' -c '%s'", exe, serverURL, serverCert),
			"production", "", "", appLogger)
		if err != nil {
			die("failed to enable job submission: %s", err)
		}

		info("opening database, please wait...")

		err = s.LoadSetDB(args[0])
		if err != nil {
			die("failed to load database: %s", err)
		}

		defer s.Stop()

		sayStarted()

		err = s.Start(serverURL, serverCert, serverKey)
		if err != nil {
			die("non-graceful stop: %s", err)
		}
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

	return &log15Writer{logger: appLogger}
}

// logToSyslog sets our applogger to log to syslog, dies if it can't.
func logToSyslog() {
	fh, err := log15.SyslogHandler(syslog.LOG_INFO|syslog.LOG_DAEMON, "ibackup-server", log15.LogfmtFormat())
	if err != nil {
		die("failed to log to syslog: %s", err)
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

// prepareAuth sets up a token for our own clients to log in with, then enables
// auth allowing that token for us, or LDAP for normal users.
func prepareAuth(s *server.Server) {
	user, err := user.Current()
	if err != nil {
		die("%s", err)
	}

	serverUser = user.Username
	serverUID = user.Uid

	tokenPath, err := tokenStoragePath()
	if err != nil {
		die("%s", err)
	}

	serverToken, err = gas.GenerateAndStoreTokenForSelfClient(tokenPath)
	if err != nil {
		die("failed to generate a token: %s", err)
	}

	err = s.EnableAuth(serverCert, serverKey, checkPassword)
	if err != nil {
		die("failed to enable authentication: %s", err)
	}
}

// tokenStoragePath returns the path where we store our token for self-clients
// to use.
func tokenStoragePath() (string, error) {
	dir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, serverTokenBasename), nil
}

// checkPassword returns true if myselfLoggingIn(), otherwise defers to
// checkLDAPPassword().
func checkPassword(username, password string) (bool, string) {
	if myselfLoggingIn(username, password) {
		return true, serverUID
	}

	return checkLDAPPassword(username, password)
}

// myselfLoggingIn checks if the supplied user is ourselves, and the password is
// the unique token generated when we started the server and stored in a file
// only readable by us. If it is, we return true.
func myselfLoggingIn(username, password string) bool {
	if username != serverUser {
		return false
	}

	return gas.TokenMatches([]byte(password), serverToken)
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
