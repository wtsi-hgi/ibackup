# ibackup
Performant backups of many files to iRODS.

ibackup is used to copy files from a locally mounted volume to collections in
iRODS, with some useful metadata.

It is pretty much as fast as you can get (as fast as irsync) for transferring
individual files (as opposed to tar'ing stuff up and putting the tar ball), and
can be used for transfers of even millions of files without issue.

If a desired file had previously been uploaded to iRODS by ibackup, and its
mtime hasn't changed, it will be skipped; otherwise it will be overwritten.

The different ibackup subcommands have documentation; just use the -h  option to
read it and find out about other options.

## Status
In active development, but currently being used in production by admins. Not
quite "nice" enough for end-users to use yet.

## Installation
Given an installation of go in your PATH, clone the repo, cd to it, and:

```
make
```

NB: `make` requires CGO, but you can build a statically compiled pure-go binary
if your group information isn't stored in LDAP.

Then copy the resulting executable to somewhere in your PATH.

## Requirements
https://github.com/wtsi-npg/baton must also be installed and in your PATH.
This has been tested with v4.0.1.

To use the server mode, you will also need:
https://github.com/VertebrateResequencing/wr
installed and configured to work on your system.

## Server Usage
Server usage is the recommended way of dealing with backups. The ibackup server
should be started by a user with permissions to read all local files and write
to all desired collections. Subsequently, end-users or a
service-user-on-behalf-of-an-end-user can add backup sets which the server will
then process.

You will need an ssl key and cert. You can generate self-signed ones like
(replace "internal.domain" to the host you're run the server on):

```
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -subj '/CN=internal.domain' -addext "subjectAltName = DNS:internal.domain" -nodes
```

Start the server process like this (changing the -s and -l option as appropriate
for your own LDAP):

```
export IBACKUP_SERVER_URL='internal.domain:4678'
export IBACKUP_SERVER_CERT='/path/to/cert.pem'
export no_proxy=localhost,127.0.0.1,.internal.domain

wr manager start
wr limit -g irods:10

ibackup server -c cert.pem -k key.pem --logfile log -s ldap-ro.internal.sanger.ac.uk -l 'uid=%s,ou=people,dc=sanger,dc=ac,dc=uk' set.db &
```

Then you can add a new backup set on behalf of a user (setting -t as
appropriate):

```
export IBACKUP_SERVER_URL='internal.domain:4678'
export IBACKUP_SERVER_CERT='/path/to/cert.pem'
export no_proxy=localhost,127.0.0.1,.internal.domain

ibackup add --user <the user's username> -n <a good name for the backup set> -t 'prefix=local:remote' [then -d, -f or -p to specify what to backup; see -h for details]
```

You can view the status of backups with:

```
ibackup status --user <the user's username>
```
### Monitoring

With the addition of the `--monitor` flag to the add command, you can enable
monitoring of the set.

The `--monitor` flags takes a time period (e.g. 1h for 1 hour) to specify how
long after the last set completion time you wish the set to be checked again.

For example, the following command will add a monitored set, that will be
monitored every 72 hours:

```
ibackup add -n monitored_set -p /directory/with/files --monitor 72h
```

In this example, every 72 hours, the monitor will activate, rechecking the
contents of the directory, and will upload any new or modified files to iRODS.

NB: This will not remove files already uploaded to iRODS that were removed from
the local directory.

## Manual Usage
Instead of using the server, for simple one-off backup jobs you can manually
create files listing what you want backed up, and run ibackup put jobs yourself,
Eg:

```
find /dir/that/needsbackup -type f -print0 | ibackup addremote -p '/dir/that/needsbackup:/zone/collection' -0 -b > local.remote
shuf local.remote > local.remote.shuffled
split -l 10000 local.remote.shuffled chunk.

ls chunk.* | perl -ne 'chomp; print "ibackup put -b -f $_ > $_.out 2>&1\n"' > put.jobs
```

Now you have a file containing ibackup put jobs that each will quickly upload
up to 10,000 files. Run those jobs however you like, eg. by adding them to wr:

```
wr add -f put.jobs -i needsbackup -g ibackup -m 1G -t 8h -r 3 -l 'irods' --cwd_matters
```
