package edu.utexas.tacc.tapis.files.lib.clients;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
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

import edu.utexas.tacc.tapis.globusproxy.client.gen.model.GlobusFileInfo;
import edu.utexas.tacc.tapis.globusproxy.client.gen.model.ReqMakeDir;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;

import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.globusproxy.client.GlobusProxyClient;
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
  private static final String pat = "yyyy-MM-dd HH:mm:ssxxx";

  private final Logger log = LoggerFactory.getLogger(GlobusDataClient.class);

  private static final String DEFAULT_GLOBUS_ROOT_DIR = "/~/";

  private final String oboTenant;
  private final String oboUser;
  private final TapisSystem system;
  private final String rootDir;
  private final GlobusProxyClient client;
  private final String accessToken;
  private final String refreshToken;

  private final ServiceClients serviceClients;

  @Override
  public void reserve() {}
  @Override
  public void release() {}

  public String getApiTenant() { return oboTenant; }
  public String getApiUser() { return oboUser; }
  public String getSystemId() { return system.getId(); }
  public GlobusProxyClient getClient() { return client; }

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
    serviceClients = serviceClients1;
    // Make sure we have a valid rootDir that is not empty and begins with /
    String tmpDir = system.getRootDir();
    if (StringUtils.isBlank(tmpDir)) tmpDir = DEFAULT_GLOBUS_ROOT_DIR;
    rootDir = StringUtils.prependIfMissing(tmpDir,"/");

    // Get the client for the tenant+user
    client = getGlobusProxyClient(oboTenant, oboUser);
    if (system.getAuthnCredential() != null)
    {
      accessToken = system.getAuthnCredential().getAccessToken();
      refreshToken = system.getAuthnCredential().getRefreshToken();
    }
    else
    {
// TODO     accessToken = null;
// TODO     refreshToken = null;
// TODO *****************************************************************
// TODO *****************************************************************
// TODO *****************************************************************
      // TODO remove hard coded access token
// TODO *****************************************************************
// TODO *****************************************************************
// TODO *****************************************************************
      accessToken = "AgroVw5qp5erwknQl0wzwYvOEbJ3mqn7Vvd5EazX52G8Wv5OQ2CXCOykJJwwaoMWW84aKBQwwOOk6zHleE5rwin2aj";
      refreshToken = "AgV7mPmvv44x1eVezOQ9a8QxePg52Ej6QMXDkNrnY8dBEXGzrJuaUW8B49wQ1DxOyQvomPoEjwG96l4jvgxmj7pK50oEl";
    }

    // Throw IOException if credentials are missing
    if (StringUtils.isBlank(accessToken) || StringUtils.isBlank(refreshToken))
    {
    String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_CRED_ERR", oboTenant, oboUser, system.getId());
    throw new IOException(msg);
    }
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
    // TODO ************************************************************************************
    String clientId = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"; // Client for scblack
//TODO    String endpointId = system.getHost();
    String endpointId = "4549fadc-7941-11ec-9f32-ed182a728dff"; // Endpoint scblack-test-laptop
    String tmpPath = "/~/data/globus"; // File on scblack-test-laptop, /~/data/globus/test1.txt
//    tmpPath="/data";
    // TODO ************************************************************************************

    // Use pre-configured Tapis clientId
// TODO    String clientId = getClientId(opName);

    // Convert limit and offset to int for call to Globus.
    // As long as MAX_LISTING_SIZE is less than Integer max that is ok
    int count = (int) Math.min(limit, MAX_LISTING_SIZE);
    int startIdx = (int) Math.min(offset, MAX_LISTING_SIZE);
    startIdx = Math.max(startIdx, 0);
    List<FileInfo> filesList = new ArrayList<>();
    // Process the relative path string and make sure it is not empty.
    String relPathStr = PathUtils.getRelativePath(path).toString();
    Path absolutePath = PathUtils.getAbsolutePath(rootDir, relPathStr);

    // TODO
    relPathStr = tmpPath;
    String filterStr = null;

    try
    {
      // TODO for now use hard coded values for cliendId, endpointId, path
      var globusFilesList = client.listFiles(clientId, endpointId, accessToken, refreshToken,
                                                               tmpPath, count, startIdx, filterStr);
      for (GlobusFileInfo globusFileInfo : globusFilesList)
      {
        FileInfo fileInfo = new FileInfo();
        // Set the fileInfo attributes from the retrieved globus files
        fileInfo.setName(globusFileInfo.getName());
        fileInfo.setPath(globusFileInfo.getPath());
        fileInfo.setGroup(globusFileInfo.getGroup());
        fileInfo.setOwner(globusFileInfo.getUser());
        // NOTE: It appears globus is using RFC 3339 rather than ISO 8601.
        //       RFC 3339 allows either a space or a 'T' to separate date and time.
        //       ISO 8601 requires a 'T'
        // If we have a last_modified timestamp then convert it to an instant
        if (!StringUtils.isBlank(globusFileInfo.getLastModified()))
        {
          Instant lastModified =
             OffsetDateTime.parse(globusFileInfo.getLastModified(), DateTimeFormatter.ofPattern(pat)).toInstant();
          fileInfo.setLastModified(lastModified);
        }
        fileInfo.setNativePermissions(globusFileInfo.getPermissions());
        if (globusFileInfo.getSize() != null)
          fileInfo.setSize(globusFileInfo.getSize());
        // Set the type
        // NOTE: currently can use the type from Globus because they match our defined types of
        //       FileInfo.FILETYPE_FILE and FileInfo.FILETYPE_DIR.
        fileInfo.setType(globusFileInfo.getType());
        filesList.add(fileInfo);
      }
    }
    catch (TapisClientException e)
    {
        String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
                                     endpointId, relPathStr, e.getMessage());
        throw new IOException(msg, e);
    }
    filesList.sort(Comparator.comparing(FileInfo::getName));
    return filesList.stream().skip(startIdx).limit(count).collect(Collectors.toList());
  }

  /**
   * Create a directory on the Globus endpoint
   *
   * @param path - Path to directory relative to the system rootDir
   * @throws IOException Generally a network error
   */
  @Override
  public void mkdir(@NotNull String path) throws IOException
  {
    String opName = "mkDir";
    String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
    // TODO ************************************************************************************
    String clientId = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"; // Client for scblack
//TODO    String endpointId = system.getHost();
    String endpointId = "4549fadc-7941-11ec-9f32-ed182a728dff"; // Endpoint scblack-test-laptop
    String tmpPath = "/~/data/globus/test_dir_new"; // File on scblack-test-laptop, /~/data/globus/test1.txt
    // TODO ************************************************************************************

    // Use pre-configured Tapis clientId
// TODO    String clientId = getClientId(opName);

    String status = null;
    try
    {
      // TODO for now use hard coded values for cliendId, endpointId, path
      status = client.makeDir(clientId, endpointId, accessToken, refreshToken, tmpPath);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
                                   endpointId, path, e.getMessage());
      throw new IOException(msg, e);
    }
      // TODO/TBD check status string returned by client?
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
    String oldAbsolutePath = PathUtils.getAbsolutePath(rootDir, oldPath).toString();
    String newAbsolutePath = PathUtils.getAbsolutePath(rootDir, newPath).toString();
    // TODO ************************************************************************************
    String clientId = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"; // Client for scblack
//TODO    String endpointId = system.getHost();
    String endpointId = "4549fadc-7941-11ec-9f32-ed182a728dff"; // Endpoint scblack-test-laptop
    // TODO ************************************************************************************

    // Use pre-configured Tapis clientId
// TODO    String clientId = getClientId(opName);

    String status = null;
    try
    {
      // TODO for now use hard coded values for cliendId, endpointId
      status = client.renamePath(clientId, endpointId, accessToken, refreshToken, oldAbsolutePath, newAbsolutePath);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR2", oboTenant, oboUser, opName, system.getId(),
                                   endpointId, oldPath, newPath, e.getMessage());
      throw new IOException(msg, e);
    }
      // TODO How to check if path exists?
//      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE))
//      {
//        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", apiTenant, apiUser, systemId, effectiveUserId, host, rootDir, relPathStr);
//        throw new NotFoundException(msg);
//      }
//      else
//      {
//        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", apiTenant, apiUser, "ls", systemId, effectiveUserId, host, relPathStr, e.getMessage());
//        throw new IOException(msg, e);
//      }
      // TODO/TBD check status string returned by client?
// TODO **********************************************************************************************************
/*  From SSHDataClient move() method
    String relOldPathStr = PathUtils.getRelativePath(srcPath).toString();
    String relNewPathStr = PathUtils.getRelativePath(dstPath).toString();
    Path absoluteOldPath = PathUtils.getAbsolutePath(rootDir, relOldPathStr);
    Path absoluteNewPath = PathUtils.getAbsolutePath(rootDir, relNewPathStr);
    SSHSftpClient sftpClient = connectionHolder.getSftpClient();
    try
    {
      // If newPath is an existing directory then append the oldPath file/dir name to the newPath
      //  so the oldPath is moved into the target directory.
      FileInfo fileInfo = getFileInfo(sftpClient, relNewPathStr);
      if (fileInfo != null && fileInfo.isDir()) {
        absoluteNewPath = Paths.get(absoluteNewPath.toString(), absoluteOldPath.getFileName().toString());
      }
      sftpClient.rename(absoluteOldPath.toString(), absoluteNewPath.toString());
    }
    catch (IOException e)
    {
      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE)) {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", apiTenant, apiUser, systemId, effectiveUserId, host, rootDir, srcPath);
        throw new NotFoundException(msg);
      } else {
        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR2", apiTenant, apiUser, "move", systemId, effectiveUserId, host, srcPath, dstPath, e.getMessage());
        throw new IOException(msg, e);
      }
    }
    finally
    {
      sftpClient.close();
      connectionHolder.returnSftpClient(sftpClient);
    }
 */
// TODO **********************************************************************************************************
  }

  /**
   * Delete a Globus file or directory
   *
   * @param path - Path to object relative to the system rootDir
   * @throws NotFoundException if path not found
   * @throws IOException on error
   */
    @Override
    public void delete(@NotNull String path) throws IOException, NotFoundException
    {
      String opName = "delete";
      String absolutePath = PathUtils.getAbsolutePath(rootDir, path).toString();
      // TODO ************************************************************************************
      boolean recurse = true; // TODO/TBD: ??? check if file or dir then set to true only if dir? or can be true for either?
      String clientId = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"; // Client for scblack
// TODO   String endpointId = system.getHost();
      String endpointId = "4549fadc-7941-11ec-9f32-ed182a728dff"; // Endpoint scblack-test-laptop
      // TODO ************************************************************************************

      var reqMakeDir = new ReqMakeDir();
      reqMakeDir.setPath(absolutePath);

      // Use pre-configured Tapis clientId
// TODO    String clientId = getClientId(opName);

      String status = null;
      try
      {
        // TODO for now use hard coded values for cliendId, endpointId
        status = client.deletePath(clientId, endpointId, accessToken, refreshToken, absolutePath, recurse);
      }
      catch (TapisClientException e)
      {
        String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
                                     endpointId, path, e.getMessage());
        throw new IOException(msg, e);
      }
        // TODO How to check if path exists?
//      if (e.getMessage().toLowerCase().contains(NO_SUCH_FILE))
//      {
//        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_NOT_FOUND", apiTenant, apiUser, systemId, effectiveUserId, host, rootDir, relPathStr);
//        throw new NotFoundException(msg);
//      }
//      else
//      {
//        String msg = LibUtils.getMsg("FILES_CLIENT_SSH_OP_ERR1", apiTenant, apiUser, "ls", systemId, effectiveUserId, host, relPathStr, e.getMessage());
//        throw new IOException(msg, e);
//      }
        // TODO/TBD check status string returned by client?
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
    FileInfo fileInfo = null;
    // Process the relative path string and make sure it is not empty.
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    String absolutePathStr = PathUtils.getAbsolutePath(rootDir, relativePathStr).toString();
    // TODO ************************************************************************************
    String clientId = "0259148a-8ae0-44b7-80b5-a4060e92dd3e"; // Client for scblack
//TODO    String endpointId = system.getHost();
    String endpointId = "4549fadc-7941-11ec-9f32-ed182a728dff"; // Endpoint scblack-test-laptop
    String tmpPath = "data/globus/test1.txt"; // File on scblack-test-laptop, /~/data/globus/test1.txt

    // Use pre-configured Tapis clientId
// TODO    String clientId = getClientId(opName);

    int count = 1;
    int startIdx = 0;
    String filterStr = "name:="+ absolutePathStr;
    // TODO ************************************************************************************
    // TODO for now use hard coded values for cliendId, endpointId, path and recurse
    List<GlobusFileInfo> globusFilesList;
    try
    {
      globusFilesList = client.listFiles(clientId, endpointId, accessToken, refreshToken, absolutePathStr, count, startIdx, filterStr);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR", oboTenant, oboUser, opName, system.getId(),
              endpointId, absolutePathStr, e.getMessage());
      throw new IOException(msg, e);
    }
    if (globusFilesList == null || globusFilesList.isEmpty()) return null;
    // There should be only one item
    if (globusFilesList.size() != 1)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_GLOBUS_OP_ERR3", oboTenant, oboUser, opName, system.getId(),
                                   endpointId, absolutePathStr, globusFilesList.size());
      throw new IOException(msg);
    }

    var globusFileInfo = globusFilesList.get(0);
    fileInfo = new FileInfo();
    // Set the fileInfo attributes from the retrieved globus files
    fileInfo.setName(globusFileInfo.getName());
    fileInfo.setPath(globusFileInfo.getPath());
    fileInfo.setGroup(globusFileInfo.getGroup());
    fileInfo.setOwner(globusFileInfo.getUser());
    // If we have a last_modified timestamp then convert it to an instant
    if (!StringUtils.isBlank(globusFileInfo.getLastModified()))
    {
      Instant lastModified =
              OffsetDateTime.parse(globusFileInfo.getLastModified(), DateTimeFormatter.ofPattern(pat)).toInstant();
      fileInfo.setLastModified(lastModified);
    }
    fileInfo.setNativePermissions(globusFileInfo.getPermissions());
    if (globusFileInfo.getSize() != null)
      fileInfo.setSize(globusFileInfo.getSize());
    // Set the type
    // NOTE: currently can use the type from Globus because they match our defined types of
    //       FileInfo.FILETYPE_FILE and FileInfo.FILETYPE_DIR.
    fileInfo.setType(globusFileInfo.getType());

    return fileInfo;
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

  /**
   * Get the configured Globus Client Id
   * @param opName - operation, for logging
   * @return clientId
   * @throws IOException If clientId not found in runtime settings.
   */
  private String getClientId(String opName) throws IOException
  {
    String clientId = RuntimeSettings.get().getGlobusClientId();
    // If no clientId configured then throw an exception
    if (StringUtils.isBlank(clientId))
      throw new IOException(LibUtils.getMsg("FILES_CLIENT_GLOBUS_NO_CLIENTID", oboTenant, oboUser, opName, system.getId()));
    return clientId;
  }
}
