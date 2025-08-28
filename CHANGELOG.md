# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this
project adheres to [Semantic Versioning](http://semver.org/).


## [1.11.0] - 2025-08-28
### New
- `ibackup sync` command with optional `--delete` option to backup a set
  again (eg. when monitoring is not enabled).
- `ibackup list --last-state` flag to list uploaded files that also existed
  locally the last time the set was backed up, to aid disaster recovery.
- Remaining options added to `ibackup edit` to match what can be set with `add`.
- Admin-only ability to to `ibackup edit --add-items`.
- Admin-only ability to remove trashed files.

### Changed
- Requests for files to be deleted from iRODS now results in them being
  "trashed", allowing them to be recovered in the case of mistakes.

### Fixed
- Cases of server startup getting stuck


## [1.10.2] - 2025-07-09
### New
- `ibackup remove` feature to remove backed up files from iRODS.
- `ibackup edit` can now edit transformer, and add a file/dir to a set.


## [1.10.1] - 2025-07-07
### New
- Added ability to "hide" sets from the default `ibackup status` view.


## [1.10.0] - 2025-07-04
### Changed
- Discovery no longer removes no-longer-existing local files from the ibackup
  database; we keep a record of everything we uploaded that is still in iRODS.


## [1.9.0] - 2025-06-20
### New
- `ibackup get --hardlinks_as_normal` to always download "hardlink" files as
  normal files, possibly duplicated data locally.


## [1.8.9] - 2025-06-19
### New
- `ibackup edit --description` option added.

### Fixed
- Server no longer tries to recover read-only sets.


## [1.8.8] - 2025-06-17
### New
- `ibackup edit` feature to alter properties of existing sets. Not all options
  in `add` have been implemented yet. Only disabling of monitoring, archiving
  and making sets read-only (and admins can make them writable again).

### Changed
- The humgen transformer now supports _v2 paths.


## [1.8.7] - 2025-06-17
### Fixed
- `ibackup get` has improved permissions on created directories, and copes when
  the remote file doesn't have mtime metadata.


## [1.8.6] - 2025-06-12
### Fixed
- `ibackup get` now uses symlink-safe versions of Chtimes and Chown.


## [1.8.5] - 2025-06-12
### Fixed
- `ibackup get` now overwrites symlinks.


## [1.8.4] - 2025-06-11
### Fixed
- `ibackup get` downloads to a temp-file before renaming to correct filename,
  to cope with interrupted downloads and resuming reliably.


## [1.8.3] - 2025-06-10
### Fixed
- `ibackup get` no longer skips empty local files.


## [1.8.2] - 2025-06-10
### New
- `ibackup list --base64` option, for supplying unusual file name to `get`.


## [1.8.1] - 2025-06-09
### New
- `ibackup list` has new options `--all`, `--size` and `--uploaded`.


## [1.8.0] - 2025-06-06
### New
- `ibackup get` command to recover files.

### Changed
- You can no longer re-add sets with the same name.


## [1.7.5] - 2025-05-29
### New
- Sets can be made "read only", to stop them being altered, discovered or
  uploaded anymore.

### Changed
- You can no longer creates sets without a name or containing commas.

### Fixed
- Database load time improved.


## [1.7.4] - 2025-04-30
### Fixed
- Setting of incorrect metadata


## [1.7.3] - 2025-04-09
### Fixed
- Correct loggin format


## [1.7.2] - 2025-04-08
### Fixed
- Parallel metadata retrieval issue that resulted in invalid metadata being
  stored.


## [1.7.1] - 2025-01-22
### New
- Server can now send slack messages about server status.

### Fixed
- Ensure iRODS connections get closed.


## [1.7.0] - 2025-01-07
### New
- `ibackup add` has new `--reason`, `--review` and `--remove` options to add
  metadata around when files in a set should be reviewed for removal or actually
  removed.
- `ibackup add` has new `--items` option which can take a file that mixes file
  and directory paths, for smaller sets.
- `ibackup server` has new `--slack_debounce` option to set the message debounce
  period.

### Changed
- `ibackup status` now shows any specified `--metadata` for the set, along with
  reason, review and remove dates.


## [1.6.0] - 2024-12-17
### New
- `ibackup add` has new `--metadata` option to add custom metadata to files
  uploaded in a set.

### Changed
- Slack messages for connections being open are now debounced.
- Sets that are considered "complete" because no further upload attempts will be
  made on repeatedly failed uploads now indicate the failures in the "status:
  complete" message.


## [1.5.0] - 2024-11-25
### New
- New `ibackup list` command to get
- New `ibackup status --remotepaths` flag to also list remote paths.

### Changed
- Slack messaging improved.
- Sets listed by `ibackup status` are now sorted alphabetically be default, with
  a new `--order recent` option to order them by most recently discovered last.


## [1.4.0] - 2024-10-08
### New
- New slack messaging for logging important events.


## [1.3.0] - 2024-07-03
### New
- Added gengen transformer.


## [1.2.1] - 2024-03-27
### Changed
- Logs to stderr if bad log file provided.
- Moved login-related code to gas dependency.


## [1.2.0] - 2023-11-24
### Added
- New filtering options for `status` command: `--complete`, `--failed` and
  `--queued`.

### Fixed
- When files are removed from sets while their uploads are still queued, they
  no longer remain in the queue forever.


## [1.1.0] - 2023-11-16
### Added
- Admin-only filestatus sub-command to print out the status of a file for each
  set the file appears in.
- Admin-only summary sub-command to summarise usage of all sets.

### Security
- Dependencies updated to avoid potential security vulnerabilites.


## [1.0.3] - 2023-07-05
### Fixed
- Inodes that get re-used won't be mis-characterised as hardlinks, nor will we
  over-write existing backup data for hardlinks with different hardlink data
  for a re-used inode.


## [1.0.2] - 2023-06-30
### Changed
- Non-regular, non-symlink files are marked as 'abnormal' and not uploaded if
  specified in a fofn, and ignored completely if in a directory.


## [1.0.1] - 2023-06-28
### Fixed
- Broken symlinks now get uploaded.


## [1.0.0] - 2023-06-01
### Changed
- First release version. Copy from local disk to iRODS implemented, manually
  and via client->server. Hardlink and symlink support.
