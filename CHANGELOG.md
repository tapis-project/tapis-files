# Change Log for Tapis Files Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/files.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

---------------------------------------------------------------------------
## 1.5.10 - 2023-11-20

Incremental improvements and new features.

### New features:

### Bug fixes:
- Rebuild with latest shared code to fix JWT validation issue.

### Breaking Changes
- None

---------------------------------------------------------------------------
## 1.5.0 - 2023-10-09

Incremental improvements and new features.

### New features:

### Bug fixes:
- Fixed an issue where ssh commands issued by the files service could fail due to a race condition causing the connection to be closed
- Fixed an NPE in mkdir in certain situations

### Breaking Changes
- None

---------------------------------------------------------------------------
## 1.4.3 - 2023-09-25

Incremental improvements and new features.

### New features:
- Use logback.xml outside the tapis files jar, and added additional logging for ssh session pool.
- Added additional logging for linux methods (/v3/files/utils/linux/*).

### Bug fixes:
- Fixed an issue where upload and getStatInfo were not working properly for publicly shared systems.
- Fixed an issue where upload was incorrectly interpreting symlinked directories as files.
- Fixed a race condition in mkdir

### Breaking Changes
- None

---------------------------------------------------------------------------
## 1.4.2 - 2023-08-30

Incremental improvements and new features.

### New features:

### Bug fixes:
- Fixed an issue in file listings / file transfers that could result in duplication of results.
- Fixed an issue in sharing support - Site admin tenant not set in worker service.
- Fixed an issue where transfers continue to retry when effectiveUserId does not have permission to write to a directory.

### Breaking Changes
- None

---------------------------------------------------------------------------
## 1.4.1 - 2023-8-4

Incremental improvements and new features.

### New features:
- Update file path authorization checks to include system sharing. Facilitates sharing an application.

### Bug fixes:
- Fix issue with handling path /~/ for GLOBUS type systems
- Fix issue where no ssh sessions are available
- Fix issue where listing contents of directories containing many subdirectories could result in incomplete results
- Fix issue where transfers containing source and destination paths that are identical could corrupt the file.

### Breaking Changes
- None

---------------------------------------------------------------------------
## 1.4.0 - 2023-07-10

Incremental improvements and new features.

### New features:
- None

### Bug fixes:
- Improvements in caching of system credentials to minimize lag when updating credentials.
- Improved HTTP return statuses in files API.

### Breaking Changes
- None

---------------------------------------------------------------------------
## 1.3.6 - 2023-05-30

Incremental improvements and new features.

### New features:
- Support file operations and transfers for systems of type GLOBUS.
- Improve support of symbolic links on linux systems.

### Bug fixes:
- Corrected an issue where irods username/passords were not retrieved properly.

### Breaking Changes
- IRODS previously used accessKey and accessSecret from system credentials, and did not
  pay any attention to a system's default authn method.  Now IRODS systems must have
  the default authn method set to "PASSWORD", and credentials for that system must use
  the password field to set the irods password, and loginUser if the irods user is 
  different than the tapis user.

---------------------------------------------------------------------------
## 1.3.5 - 2023-04-18

Shorten system cache timeout from 5 minutes to 10 seconds.

---------------------------------------------------------------------------
## 1.3.4 - 2023-04-13

Incremental improvements and bug fixes.

### New features:
- None

### Bug fixes:
- Fixed a thread leak, and case where we were not cleaning up temporary control queues for file transfers

---------------------------------------------------------------------------
## 1.3.3 - 2023-04-02

Incremental improvements and bug fixes.

### New features:
- None

### Bug fixes:
- Update sharedAppCtx to represent app share grantor. Fix for privilege escalation.

---------------------------------------------------------------------------
## 1.3.2 - 2023-3-21

Incremental improvements and bug fixes.

### New features:
- None

### Bug fixes:
- Fixed issues connecting to rabbitmq on worker startup
- Allow db connection pool to be configurable, and fix issue causing long running transactions.
- Fixed issue where when deleting a file, an error could be returned even though the file was successfully deleted.
- Fixed issue where zip entry for single file did not contain the name of file when downloading a single file.
- When downloading the system root directory the file will now be called systemRoot.zip rather than zip
- When downloading a file, skip the authorization check when retrieving the system.
 
---------------------------------------------------------------------------
## 1.3.1 - 2023-03-07

Incremental improvements and bug fixes.

### New features:
- None

### Bug fixes:
- Fixed issue where linux directories could not be copied.
- Fixed issue where path permissions cache was not being invalidated when permissions where changed

---------------------------------------------------------------------------
## 1.3.0 - 2023-02-27

Incremental improvements.

### New features:
- Added PostIts feature.  This feature allows users to create a link that 
  can be shared allowing files or directories to be downloaded without 
  authentication (limited by time and/or usage count)
- Added endpoint to get/set file acls on Linux systems

### Bug fixes:
- None

---------------------------------------------------------------------------
## 1.2.8 - 2023-01-14

Incremental improvements. Improved logging.

### New features:
- None

### Bug fixes:
- Fix bug in expiring of system cache entry when ssh connection fails.

---------------------------------------------------------------------------
## 1.2.7 - 2022-11-14

Incremental improvements and bug fixes.

### New features:
- Invalidate SSH cache entry when there is an error. Error may get resolved.
  For example, error could be due to invalid credentials or no route to host.
- Support zip download for system *rootDir* as long as not all files on host would be included.
- For certain endpoints that take a path make the trailing slash optional when dealing with top level directory: getContents, getListing, sharing.

### Bug fixes:
- When there are multiple transfer elements in a request and one finishes quickly it is possible for some to be missed.
- Fix problems with zip downloads. Require zip when path is directory. Disallow zip if request would include all files on host.
- Fix bugs in updating start times for top level task and parent tasks.
- Fix bug: Listing and download not authorized when path is shared.

---------------------------------------------------------------------------
## 1.2.6 - 2022-10-20

Incremental improvements and bug fix.

### New features:
- Review, refactor and strengthen transfer code. Additional integration tests.

### Bug fixes:
- Fix issue with S3 to LINUX transfer. Destination paths were being constructed incorrectly.
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
