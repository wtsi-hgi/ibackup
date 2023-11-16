# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this
project adheres to [Semantic Versioning](http://semver.org/).


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
