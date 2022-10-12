# Change Log for Tapis Files Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/files.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

---------------------------------------------------------------------------
## 1.2.6 - 2022-10-11

Incremental improvements and bug fix.

### New features:
- Review, refactor and strengthen transfer code. Additional integration tests.

### Bug fixes:
- Fix issue with S3 to LINUX transfer. Destination path was being constructed incorrectly.
- Fix issue with S3 path contained in FileInfo. Should be relative path and not absolute path.
- Fix issues with populating path and url in FileInfo objects for all system types.

---------------------------------------------------------------------------
## 1.2.5 - 2022-09-26

Incremental improvements and preview of new feature.

### New features:
- Support sharedAppCtx for getContents operation.

### Bug fixes:
- Remove hard coded default value for TAPIS_SITE_ID. Site ID must now be set in the environment at service start.

---------------------------------------------------------------------------
## 1.2.4 - 2022-09-08

Incremental improvements and preview of new feature.

### New features
- Support sharedAppCtx for listFiles operation.
- Add warning messages for when permission is denied.
- Add error messages for service exceptions.

### Bug fixes:
- None.

---------------------------------------------------------------------------
## 1.2.3 - 2022-08-30

Incremental improvements and preview of new feature.

### New features:
- Support sharing of a path with one or more users. Includes support for sharing with all users in a tenant.
- Support sharedAppCtx for mkdir operation and transfer requests (srcSharedAppCtx, destSharedAppCtx).
- Perform DB migration at service startup.

### Bug fixes:
- Update so MODIFY permission implies READ.

---------------------------------------------------------------------------
## 1.2.2 - 2022-07-06

Incremental improvements, new preview features and bug fix.

### New features:
- Add support for impersonationId query parameter to getTransferTaskDetails.
- Improve handling of paths in S3 support. Keys updated to never start with "/".

### Bug fixes:
- Fix issue with S3 support by updating to the latest AWS java libraries.

---------------------------------------------------------------------------
## 1.2.1 - 2022-06-21

Incremental improvements and bug fix.

### New features:
- None.

### Bug fixes:
- Fix issue with getSKClient() that breaks associate site support.

---------------------------------------------------------------------------
## 1.2.0 - 2022-05-23

Incremental improvements and preview of new feature.

### New features:
- Support impersonationId for service to service requests.

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.1.4 - 2022-05-07

Incremental improvements and preview of new feature.

### New features:
- Replace skipTapisAuthorization with impersonationId for requests from Jobs and Apps services.

### Bug fixes:
- None

---------------------------------------------------------------------------
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
