# Change Log for Tapis Files Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/files.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

## 1.1.3 - 2022-04-08

Preview of new feature.

### New features:
- Support skipTapisAuthorization for requests from Jobs and Apps services.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.1.2 - 2022-03-10

Fix issues with Jackson mapper and with worker startup..

### New features:
- None

### Updates:
- None

### Bug fixes:
- Transfer worker failing to start.
- Api throwing exceptions due to empty bean during json serialization.

---------------------------------------------------------------------------
## 1.1.1 - 2022-03-09

Updates for JDK 17.

### New features:
- None

### Updates:
- Updates for JDK 17.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.0.3 - 2022-01-08

Code cleanup and refactoring, fix issue with SSH client cache.

### New features:
- None

### Updates:
- Code cleanup and refactoring.

### Bug fixes:
- Fix concurrence problem with SSH client cache. Github issue #39

---------------------------------------------------------------------------
## 1.0.2 - 2021-12-06

Incremental improvements, support optional flag for file transfers.

### New features:
- Support optional flag for file transfers.

### Updates:
- Add debug logging of cache key loads.
- Update S3 client to mostly operate on single objects.
- Additional tests.

### Bug fixes:
- None.

---------------------------------------------------------------------------
## 1.0.1 - 2021-12-01

Incremental improvements.

### New features:
- Support copy/move of a file to a directory without having to specify full path for destination.

---------------------------------------------------------------------------
## 1.0.0 - 2021-07-16

Initial release supporting basic file operations in the Tapis ecosystem.
Users can perform file listing, uploading, operations such as move/copy/delete
and also transfer files between systems. All Tapis files APIs accept JSON as inputs.

Currently the files service includes support for S3 and SSH type file systems. Other
storage systems like IRODS will be included in future releases.

### Breaking Changes:
- Initial release.

### New features:
 - Initial release.

### Bug fixes:
- None.
