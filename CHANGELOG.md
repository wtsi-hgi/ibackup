# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this
project adheres to [Semantic Versioning](http://semver.org/).


## [1.7.0] - 2025-01-07
## New
- `ibackup add` has new `--reason`, `--review` and `--remove` options to add
  metadata around when files in a set should be reviewed for removal or actually
  removed.
- `ibackup add` has new `--items` option which can take a file that mixes file
  and directory paths, for smaller sets.
- `ibackup server` has new `--slack_debounce` option to set the message debounce
  period.

## Changed
- `ibackup status` now shows any specified `--metadata` for the set, along with
  reason, review and remove dates.


## [1.6.0] - 2024-12-17
## New
- `ibackup add` has new `--metadata` option to add custom metadata to files
  uploaded in a set.

## Changed
- Slack messages for connections being open are now debounced.
- Sets that are considered "complete" because no further upload attempts will be
  made on repeatedly failed uploads now indicate the failures in the "status:
  complete" message.


## [1.5.0] - 2024-11-25
## New
- New `ibackup list` command to get
- New `ibackup status --remotepaths` flag to also list remote paths.

## Changed
- Slack messaging improved.
- Sets listed by `ibackup status` are now sorted alphabetically be default, with
  a new `--order recent` option to order them by most recently discovered last.


## [1.4.0] - 2024-10-08
## New
- New slack messaging for logging important events.


## [1.3.0] - 2024-07-03
## New
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
