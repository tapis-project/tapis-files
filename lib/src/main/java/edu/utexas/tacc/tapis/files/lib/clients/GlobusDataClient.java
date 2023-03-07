package edu.utexas.tacc.tapis.files.lib.clients;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.globusproxy.client.gen.model.GlobusTransferTask;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.GlobusFileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;

import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.globusproxy.client.GlobusProxyClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;

/**
 * This class provides remoteDataClient file operations for Globus systems.
 * Globus operations are supported via the Tapis GlobusProxy service.
 * So our internal client is obtained via the Tapis java shared code.
 *
 * A client is constructed for a specific tenant, user and Tapis system.
 *
 * If credentials are not available then an exception is thrown during instantiation.
 *
 * Note that not all operations are supported.
 * Supported operations:
 *   - ls
 *   - mkdir
 *   - move
 *   - delete
 * Unsupported operations:
 *   - copy
 *   - upload
 *   - getStream
 *   - getBytesByRange
 */
public class GlobusDataClient implements IRemoteDataClient
{
  // Timestamp parse pattern used by Globus for lastModified, e.g. 2022-01-31 20:31:01+00:00
  // Note that this is not ISO 8601 because there is a space and not a T between data and time.
  // NOTE: It appears globus is using RFC 3339 rather than ISO 8601.
  //       RFC 3339 allows either a space or a 'T' to separate date and time.
  //       ISO 8601 requires a 'T'
  private static final String pat = "yyyy-MM-dd HH:mm:ssxxx";
  private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(pat);

  private final Logger log = LoggerFactory.getLogger(GlobusDataClient.class);

  private static final String DEFAULT_GLOBUS_ROOT_DIR = "/~/";

  private final String globusClientId;
  private final String oboTenant;
  private final String oboUser;
  private final TapisSystem system;
  private final String endpointId;
  private final String rootDir;
  private final GlobusProxyClient proxyClient;
  private final String accessToken;
  private final String refreshToken;

  private final ServiceClients serviceClients;

  // reserve and release are no-ops for this type of client.
  @Override
  public void reserve() {}
  @Override
  public void release() {}

  @Override
  public String getOboTenant() { return oboTenant; }
  @Override
  public String getOboUser() { return oboUser; }
  @Override
  public String getSystemId() { return system.getId(); }
  @Override
  public SystemTypeEnum getSystemType() { return system.getSystemType(); }
  @Override
  public TapisSystem getSystem() { return system; }

  public GlobusProxyClient getProxyClient() { return proxyClient; }

  /**
   * On instantiation create a GlobusProxyClient with credentials for given tenant+user
   * @param oboTenant1 - tenant
   * @param oboUser1 - user
   * @param system1 - system
   * @param serviceClients1 - used to get the globusProxy client
   * @throws IOException on error
   */
  public GlobusDataClient(@NotNull String oboTenant1, @NotNull String oboUser1, @NotNull TapisSystem system1,
                          ServiceClients serviceClients1)
          throws IOException
  {
    oboTenant = oboTenant1;
    oboUser = oboUser1;
    system = system1;
    endpointId = system.getHost();
    serviceClients = serviceClients1;
    // Make sure we have a valid rootDir that is not empty and begins with /
    String tmpDir = system.getRootDir();
    if (StringUtils.isBlank(tmpDir)) tmpDir = DEFAULT_GLOBUS_ROOT_DIR;
    rootDir = StringUtils.prependIfMissing(tmpDir,"/");

    // Get the client for the tenant+user
    proxyClient = getGlobusProxyClient(oboTenant, oboUser);
    if (system.getAuthnCredential() != null)
    {
      accessToken = system.getAuthnCredential().getAccessToken();
      refreshToken = system.getAuthnCredential().getRefreshToken();
    }
    else
    {
      accessToken = null;
      refreshToken = null;
    }

    // Throw IOException if credentials are missing
    if (StringUtils.isBlank(accessToken) || StringUtils.isBlank(refreshToken))
    {
      String aToken = StringUtils.isBlank(accessToken) ? "<missing>" : "***";
      String rToken = StringUtils.isBlank(refreshToken) ? "<missing>" : "***";
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_CRED_ERR", oboTenant, oboUser, system.getId(), aToken, rToken);
      throw new IOException(msg);
    }

    // Set the Globus TAPIS_GLOBUS_CLIENT_ID
    globusClientId = RuntimeSettings.get().getGlobusClientId();
    if (StringUtils.isBlank(globusClientId))
      throw new IOException(LibUtils.getMsg("FILES_CLIENT_GLOBUS_NO_ID", oboTenant, oboUser, system.getId()));
  }

  /**
   * Return all files and directories using default max limit and 0 offset
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @return list of FileInfo objects
   * @throws IOException       Generally a network error
   * @throws NotFoundException No file at target
   */
  public List<FileInfo> ls(@NotNull String path) throws IOException, NotFoundException
  {
    return ls(path, MAX_LISTING_SIZE, 0);
  }

  /**
   * Return list of directories and files
   *
   * @param path - Path to file or directory relative to the system rootDir
   * @param limit - maximum number of items to return
   * @param offset - Offset for listing
   * @return list of FileInfo objects
   * @throws IOException on error
   * @throws NotFoundException No file or directory at target path
   */
  @Override
  public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws IOException, NotFoundException
  {
    String opName = "ls";

    // Convert limit and offset to int for call to Globus.
    // As long as MAX_LISTING_SIZE is less than Integer max that is ok
    int count = (int) Math.min(limit, MAX_LISTING_SIZE);
    int startIdx = (int) Math.min(offset, MAX_LISTING_SIZE);
    startIdx = Math.max(startIdx, 0);
    List<FileInfo> filesList = new ArrayList<>();
    // Process the relative path string and make sure it is not empty.
    String relPathStr = PathUtils.getRelativePath(path).toString();
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relPathStr).toString();
    String filterStr = null;
    try
    {
      var globusFilesList = proxyClient.listFiles(globusClientId, endpointId, accessToken, refreshToken,
                                             absolutePathStr, count, startIdx, filterStr);
      for (GlobusFileInfo globusFileInfo : globusFilesList)
      {
        FileInfo fileInfo = new FileInfo();
        // Set the fileInfo attributes from the retrieved globus files
        fileInfo.setName(globusFileInfo.getName());
        fileInfo.setGroup(globusFileInfo.getGroup());
        fileInfo.setOwner(globusFileInfo.getUser());
        fileInfo.setLastModified(convertLastModified(globusFileInfo.getLastModified()));
        fileInfo.setNativePermissions(globusFileInfo.getPermissions());
        if (globusFileInfo.getSize() != null)
          fileInfo.setSize(globusFileInfo.getSize());
        // Set the type
        // NOTE: currently can use the type from Globus because they match our defined types of
        //       FileInfo.FILETYPE_FILE and FileInfo.FILETYPE_DIR.
        fileInfo.setType(globusFileInfo.getType());

        // Fill in path with relativePath as is done for other clients such as SSH, S3 and IRODS.
        fileInfo.setPath(Paths.get(relPathStr, globusFileInfo.getName()).toString());

        filesList.add(fileInfo);
      }
    }
    catch (TapisClientException e)
    {
        String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
                                     endpointId, relPathStr, absolutePathStr, e.getMessage());
        throw new IOException(msg, e);
    }
    filesList.sort(Comparator.comparing(FileInfo::getName));
    return filesList.stream().skip(startIdx).limit(count).collect(Collectors.toList());
  }

  /**
   * Create a directory on the Globus endpoint
   *
   * @param path - Path to directory relative to the system rootDir
   * @throws IOException on error
   */
  @Override
  public void mkdir(@NotNull String path) throws IOException
  {
    String opName = "mkDir";
    // Process the relative path string and make sure it is not empty.
    Path relativePath = PathUtils.getRelativePath(path);
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePath.toString()).toString();

    // Walk the path parts creating directories as we go
    Path tmpPath = Paths.get(rootDir);
    StringBuilder partRelativePathSB = new StringBuilder();
    for (Path part : relativePath)
    {
      tmpPath = tmpPath.resolve(part);
      String tmpPathStr = tmpPath.toString();
      partRelativePathSB.append(part).append('/');
      // Get file info to see if path already exists and is a dir or a file.
      // If it does not exist or exists and is a directory then all is good, if it exists and is a file it is an error
      try
      {
        FileInfo fileInfo = getFileInfo(partRelativePathSB.toString());
        if (fileInfo.isDir()) continue;
        else
        {
          String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_MKDIR_FILE", oboTenant, oboUser, system.getId(),
                                       tmpPathStr, relativePath, absolutePathStr);
          throw new BadRequestException(msg);
        }
      }
      catch (NotFoundException e) { /* Not found is good, it means mkdir should not throw an exception */ }

      // Use the client to mkdir
      String status;
      try
      {
        status = proxyClient.makeDir(globusClientId, endpointId, accessToken, refreshToken, tmpPathStr);
      }
      catch (TapisClientException e)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_MKDIR_ERR", oboTenant, oboUser, system.getId(),
                                     tmpPathStr, relativePath, absolutePathStr, e.getMessage());
        throw new IOException(msg, e);
      }
      // Report status
      log.debug(LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_STATUS", oboTenant, oboUser, opName, system.getId(),
                                endpointId, relativePath, tmpPathStr, status));
    }
  }

  /**
   * Move oldPath to newPath using globusProxy client
   * If newPath is an existing directory then oldPath will be moved into the directory newPath.
   *
   * @param oldPath current location relative to system rootDir
   * @param newPath desired location relative to system rootDir
   * @throws IOException Network errors generally
   * @throws NotFoundException Source path not found
   */
  @Override
  public void move(@NotNull String oldPath, @NotNull String newPath) throws IOException, NotFoundException
  {
    String opName = "move";
    Path oldAbsolutePath = PathUtils.getAbsolutePath(rootDir, oldPath);
    Path newAbsolutePath = PathUtils.getAbsolutePath(rootDir, newPath);
    String oldAbsolutePathStr = oldAbsolutePath.toString();
    String newAbsolutePathStr = newAbsolutePath.toString();
    // Process the relative path string and make sure it is not empty.
    String oldRelativePathStr = PathUtils.getRelativePath(oldPath).toString();
    String newRelativePathStr = PathUtils.getRelativePath(newPath).toString();

    // Make sure the old path exists
    // If it does not exist this should throw NotFound
    getFileInfo(oldRelativePathStr);

    // Get file info for destination path to see if it already exists and is a dir or a file.
    // If it does not exist or exists and is a directory then all is good, if it exists and is a file it is an error
    try
    {
      FileInfo fileInfo = getFileInfo(newRelativePathStr);
      if (fileInfo.isDir())
      {
        // newPath is an existing directory. Append the oldPath file/dir name to the newPath so the oldPath is
        // moved into the target directory.
        newAbsolutePathStr = Paths.get(newAbsolutePathStr, oldAbsolutePath.getFileName().toString()).toString();
      }
      else
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_RENAME_FILE", oboTenant, oboUser, system.getId(),
                                     oldRelativePathStr, newRelativePathStr);
        throw new BadRequestException(msg);
      }
    }
    catch (NotFoundException e) { /* Not found is good since we are doing a rename */ }

    String status = null;
    try
    {
      status = proxyClient.renamePath(globusClientId, endpointId, oldAbsolutePathStr, newAbsolutePathStr, accessToken, refreshToken);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR2", oboTenant, oboUser, opName, system.getId(),
                                   endpointId, oldPath, newPath, e.getMessage());
      throw new IOException(msg, e);
    }
    // Report status
    log.debug(LibUtils.getMsg("FILES_CLIENT_GLOBUS_MV_STATUS", oboTenant, oboUser, system.getId(), endpointId,
                              oldRelativePathStr, oldAbsolutePathStr, newRelativePathStr, newAbsolutePathStr, status));
  }

  /**
   * Delete a Globus file or directory
   * This is always a recursive delete
   *
   * @param path - Path to object relative to the system rootDir
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
    @Override
    public void delete(@NotNull String path) throws IOException, NotFoundException
    {
      String opName = "delete";
      // Required to be true if any directories involved. Not clear if setting to false ever makes sense.
      boolean recurse = true;
      // Process the relative path string and make sure it is not empty.
      Path relativePath = PathUtils.getRelativePath(path);
      Path absolutePath = PathUtils.getAbsolutePath(rootDir, path);
      String absolutePathStr = absolutePath.toString();

      // Make sure the path exists
      // If it does not exist this should throw NotFound
      FileInfo fileInfo = getFileInfo(relativePath.toString());

      String status;
      try
      {
        status = proxyClient.deletePath(globusClientId, endpointId, accessToken, refreshToken, absolutePathStr, recurse);
      }
      catch (TapisClientException e)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
                                     endpointId, path, absolutePath, e.getMessage());
        throw new IOException(msg, e);
      }
      // Report status
      log.debug(LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_STATUS", oboTenant, oboUser, opName, system.getId(),
              endpointId, relativePath, absolutePathStr, status));
    }

  /*
   * Returns file info for the file/dir or object if path exists, null if path does not exist
   *
   * @param path - path on system relative to system rootDir
   * @return FileInfo for the object or null if path does not exist.
   * @throws IOException on error
   */
  @Override
  public FileInfo getFileInfo(@NotNull String path) throws IOException
  {
    String opName = "getFileInfo";
    FileInfo fileInfo;
    // Process the relative path string and make sure it is not empty.
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    Path absolutePath = PathUtils.getAbsolutePath(rootDir, relativePathStr);
    String absolutePathStr = absolutePath.toString();
    // For Globus the listing must be for a directory and the filter is the target file/dir name.
    // Determine name of specific file or directory we want.
    String targetName = absolutePath.getFileName().toString();
    String targetDir = absolutePath.getParent().toString();
    int count = 1;
    int startIdx = 0;
    // Search for file by name
    String filterStr = "name:="+ targetName;
    List<GlobusFileInfo> globusFilesList;
    try
    {
      globusFilesList = proxyClient.listFiles(globusClientId, endpointId, accessToken, refreshToken, targetDir,
                                         count, startIdx, filterStr);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
                                   endpointId, relativePathStr, absolutePathStr, e.getMessage());
      throw new IOException(msg, e);
    }
    if (globusFilesList == null)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_NULL", oboTenant, oboUser, opName, system.getId(), relativePathStr, absolutePathStr);
      throw new IOException(msg);
    }

    // If path exists there should be only one item
    if (globusFilesList.size() == 0)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_NOT_FOUND", oboTenant, oboUser, opName, system.getId(), relativePathStr);
      throw new NotFoundException(msg);
    }
    else if (globusFilesList.size() != 1)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR3", oboTenant, oboUser, system.getId(),
                                   endpointId, relativePathStr, absolutePathStr, globusFilesList.size());
      throw new IOException(msg);
    }

    var globusFileInfo = globusFilesList.get(0);
    fileInfo = new FileInfo();
    // Set the fileInfo attributes from the retrieved globus files
    fileInfo.setName(globusFileInfo.getName());
    fileInfo.setGroup(globusFileInfo.getGroup());
    fileInfo.setOwner(globusFileInfo.getUser());
    fileInfo.setLastModified(convertLastModified(globusFileInfo.getLastModified()));
    fileInfo.setNativePermissions(globusFileInfo.getPermissions());
    if (globusFileInfo.getSize() != null)
      fileInfo.setSize(globusFileInfo.getSize());
    // Set the type
    // NOTE: currently can use the type from Globus because they match our defined types of
    //       FileInfo.FILETYPE_FILE and FileInfo.FILETYPE_DIR.
    fileInfo.setType(globusFileInfo.getType());

    // Fill in path with relativePath as is done for other clients such as SSH, S3 and IRODS.
    fileInfo.setPath(relativePathStr);

    return fileInfo;
  }

  /*
   * Use the GlobusProxyClient to create a transfer task using this client's endpoint as the source endpoint.
   * The source path, destination endpoint and destination path must be passed in
   * Return the globus transfer task id
   *
   * @param path - path on system relative to system rootDir
   * @return Globus transfer task id as a string.
   * @throws ServiceException on error
   */
  public GlobusTransferTask createTransferTaskFromEndpoint(String srcPath, String dstEndpint, String dstPath)
          throws TapisClientException
  {
    String opName = "createGlobusTransferTaskFromEndpoint";



    FileInfo fileInfo;
    // Process the relative path string and make sure it is not empty.
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    Path absolutePath = PathUtils.getAbsolutePath(rootDir, relativePathStr);
    String absolutePathStr = absolutePath.toString();
    // For Globus the listing must be for a directory and the filter is the target file/dir name.
    // Determine name of specific file or directory we want.
    String targetName = absolutePath.getFileName().toString();
    String targetDir = absolutePath.getParent().toString();
    int count = 1;
    int startIdx = 0;
    // Search for file by name
    String filterStr = "name:="+ targetName;
    TransferTask globusTransferTask;
    try
    {
      globusFilesList = proxyClient.listFiles(globusClientId, endpointId, accessToken, refreshToken, targetDir,
              count, startIdx, filterStr);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
              endpointId, relativePathStr, absolutePathStr, e.getMessage());
      throw new IOException(msg, e);
    }
    if (globusFilesList == null)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_NULL", oboTenant, oboUser, opName, system.getId(), relativePathStr, absolutePathStr);
      throw new IOException(msg);
    }

    // If path exists there should be only one item
    if (globusFilesList.size() == 0)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_NOT_FOUND", oboTenant, oboUser, opName, system.getId(), relativePathStr);
      throw new NotFoundException(msg);
    }
    else if (globusFilesList.size() != 1)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR3", oboTenant, oboUser, system.getId(),
              endpointId, relativePathStr, absolutePathStr, globusFilesList.size());
      throw new IOException(msg);
    }

    var globusFileInfo = globusFilesList.get(0);
    fileInfo = new FileInfo();
    // Set the fileInfo attributes from the retrieved globus files
    fileInfo.setName(globusFileInfo.getName());
    fileInfo.setGroup(globusFileInfo.getGroup());
    fileInfo.setOwner(globusFileInfo.getUser());
    fileInfo.setLastModified(convertLastModified(globusFileInfo.getLastModified()));
    fileInfo.setNativePermissions(globusFileInfo.getPermissions());
    if (globusFileInfo.getSize() != null)
      fileInfo.setSize(globusFileInfo.getSize());
    // Set the type
    // NOTE: currently can use the type from Globus because they match our defined types of
    //       FileInfo.FILETYPE_FILE and FileInfo.FILETYPE_DIR.
    fileInfo.setType(globusFileInfo.getType());

    // Fill in path with relativePath as is done for other clients such as SSH, S3 and IRODS.
    fileInfo.setPath(relativePathStr);

    return fileInfo;
  }

  /*
   * Use the GlobusProxyClient to get a transfer task.
   *
   * @param path - path on system relative to system rootDir
   * @return Globus transfer task status.
   * @throws TapisClientException on error
   */
  public String getGlobusTransferTaskStatus(String globusTaskId) throws TapisClientException
  {
    var transferTask = proxyClient.getTransferTask(globusClientId, accessToken, refreshToken, globusTaskId);
    String taskDetailsStr = transferTask.toString();
    log.trace(LibUtils.getMsg("FILES_TXFR_ASYNCH_ETASK", oboTenant, oboUser, globusTaskId, taskDetailsStr));
    var taskStatus = transferTask.getStatus();
    return (taskStatus == null ? null : transferTask.getStatus().name());
  }

    /* **************************************************************************** */
  /*                   Unsupported Operations                                     */
  /* **************************************************************************** */
  /**
   * Upload - not supported
   */
  @Override
  public void upload(@NotNull String path, @NotNull InputStream fileStream) throws IOException
  {
    String opName = "upload";
    String msg = LibUtils.getMsg("FILES_OPSC_UNSUPPORTED", oboTenant, oboUser, system.getSystemType(), opName,
                                 system.getId(), path);
    throw new NotImplementedException(msg);
  }

  /**
   * Copy - not supported
   */
  @Override
  public void copy(@NotNull String srcPath, @NotNull String dstPath) throws IOException, NotFoundException
  {
    String opName = "copy";
    String msg = LibUtils.getMsg("FILES_OPSC_UNSUPPORTED", oboTenant, oboUser, system.getSystemType(), opName,
                                 system.getId(), srcPath);
    throw new NotImplementedException(msg);
  }

  /*
   * getStream - not supported
   */
  @Override
  public InputStream getStream(@NotNull String path) throws IOException, NotFoundException
  {
    String opName = "getStream";
    String msg = LibUtils.getMsg("FILES_OPSC_UNSUPPORTED", oboTenant, oboUser, system.getSystemType(), opName,
                                 system.getId(), path);
    throw new NotImplementedException(msg);
  }

  /*
   * getBytesByRange - not supported
   */
  @Override
  public InputStream getBytesByRange(@NotNull String path, long startByte, long count)
  {
    String opName = "getBytesByRange";
    String msg = LibUtils.getMsg("FILES_OPSC_UNSUPPORTED", oboTenant, oboUser, system.getSystemType(), opName,
                                 system.getId(), path);
    throw new NotImplementedException(msg);
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Get GlobusProxy client associated with specified tenant and user
   * @return GlobusProxy client
   * @throws IOException - on error
   */
  private GlobusProxyClient getGlobusProxyClient(String tenantName, String userName) throws IOException
  {
    GlobusProxyClient globusProxyClient;
    try
    {
      globusProxyClient = serviceClients.getClient(userName, tenantName, GlobusProxyClient.class);
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_GLOBUSPROXY, tenantName, userName);
      throw new IOException(msg, e);
    }
    return globusProxyClient;
  }

  /*
   * Convert a globus lastModified timestamp string to an instant.
   * NOTE: It appears globus is using RFC 3339 rather than ISO 8601.
   *       RFC 3339 allows either a space or a 'T' to separate date and time.
   *       ISO 8601 requires a 'T'
   * If string is empty or null return null;
   */
  private Instant convertLastModified(String timeStr)
  {
    if (StringUtils.isBlank(timeStr)) return null;
    LocalDateTime lastModified = LocalDateTime.parse(timeStr, timeFormatter);
    return lastModified.toInstant(ZoneOffset.UTC);
  }
}
