package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/*
 * Service level methods for File Operations.
 *  -  Each public method uses provided IRemoteDataClient and other service library classes to perform all top level
 *     service operations.
 *  - Paths provided will all be treated as relative to the system rootDir. Paths will be normalized. Please see
 *    PathUtils.java.
 *
 * Annotate as an hk2 Service so that default scope for Dependency Injection is singleton
 */
@Service
public class FileOpsService implements IFileOpsService
{
  public static final int MAX_LISTING_SIZE = 1000;

  public enum MoveCopyOperation {MOVE, COPY}

  private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);
  private final FilePermsService permsService;

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_FILES;
  // 0=systemId, 1=path, 2=tenant
  private final String TAPIS_FILES_URL_FORMAT = "tapis://{0}/{1}?tenant={2}";
  private static final int MAX_RECURSION = 20;

  @Inject
  public FileOpsService(FilePermsService svc) { permsService = svc; }

  @Inject
  SystemsCache systemsCache;

  @Inject
  RemoteDataClientFactory remoteDataClientFactory;

  // We must be running on a specific site and this will never change
  // These are initialized in method initService()
  private static String siteId;
  private static String siteAdminTenantId;
  public static String getSiteId() {return siteId;}
  public static String getServiceTenantId() {return siteAdminTenantId;}
  public static String getServiceUserId() {return SERVICE_NAME;}

  /**
   * Initialize the service:
   *   init service context
   *   migrate DB
   */
  public void initService(String siteId1, String siteAdminTenantId1, String svcPassword) throws TapisException, TapisClientException
  {
    // Initialize service context and site info
    siteId = siteId1;
    siteAdminTenantId = siteAdminTenantId1;
//  TODO
//    serviceContext.initServiceJWT(siteId, APPS_SERVICE, svcPassword);
//    // Make sure DB is present and updated to latest version using flyway
//    dao.migrateDB();
  }

  // ----------------------------------------------------------------------------------------------------
  // ------------- Support for FileOps
  // ----------------------------------------------------------------------------------------------------
  /**
   * List files at path using given limit and offset
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   * @throws NotAuthorizedException - user not authorized
   */
  @Override
  public List<FileInfo> ls(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                           long limit, long offset)
          throws WebApplicationException
  {
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.READ);
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return ls(client, path, limit, offset);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", apiTenant, apiUser, "ls", sysId, path, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * List files at path using provided client and given limit and offset
   * NOTE: This method does not check permissions. Callers should check first
   * @param client - Remote data client
   * @param path - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  @Override
  public List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path, long limit, long offset)
          throws ServiceException
  {
    Path relativePath = PathUtils.getRelativePath(path);
    LibUtils.checkPermitted(permsService, client.getApiTenant(), client.getApiUser(), client.getSystemId(),
                            relativePath, Permission.READ);
    try
    {
      List<FileInfo> listing = client.ls(relativePath.toString(), limit, offset);
      listing.forEach(f ->
        {
          String url = String.format("%s/%s", client.getSystemId(), f.getPath());
          url = StringUtils.replace(url, "//", "/");
          f.setUrl("tapis://" + url);
          // Ensure there is a leading slash
          f.setPath(StringUtils.prependIfMissing(f.getPath(), "/"));
        }
      );
      return listing;
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getApiTenant(), client.getApiUser(), "lsWithClient",
                                   client.getSystemId(), path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Recursive list files at path. Max possible depth = MAX_RECURSION(20)
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @param depth - maximum depth for recursion
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public List<FileInfo> lsRecursive(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                      @NotNull String path, int depth)
            throws WebApplicationException
    {
      String apiTenant = rUser.getOboTenantId();
      String apiUser = rUser.getOboUserId();
      String sysId = sys.getId();
      Path relativePath = PathUtils.getRelativePath(path);
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.READ);
        client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
        client.reserve();
        return lsRecursive(client, path, depth);
      }
      catch (IOException | ServiceException ex)
      {
        throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "lsRecursive", sysId, path,
                                          ex.getMessage()), ex);
      }
      finally
      {
        if (client != null) client.release();
      }
    }

  /**
   * Recursive list files at path using provided client.
   * Max possible depth = MAX_RECURSION(20)
   * NOTE: This method does not check permissions. Callers should check first
   * @param client - Remote data client
   * @param path - path on system relative to system rootDir
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String path, int depth)
          throws ServiceException
  {
    List<FileInfo> listing = new ArrayList<>();
    // Make the call that does recursion
    listDirectoryRecurse(client, path, listing, 0, Math.min(depth, MAX_RECURSION));
    return listing;
  }

  /**
   * Upload a file
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @param inStrm  data stream to be used when creating file
   * @throws ForbiddenException - user not authorized
   */
  @Override
  public void upload(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                     @NotNull InputStream inStrm)
          throws WebApplicationException
  {
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.MODIFY);
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      upload(client, path, inStrm);
    }
    catch (IOException | ServiceException ex)
    {
      throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "upload", sysId, path,
                                        ex.getMessage()), ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Upload a file using provided client
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @param inputStream  data stream to be used when creating file
   * @throws ServiceException - general error
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void upload(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull InputStream inputStream)
            throws ServiceException
    {
      Path relativePath = PathUtils.getRelativePath(path);
      LibUtils.checkPermitted(permsService, client.getApiTenant(), client.getApiUser(), client.getSystemId(),
                              relativePath, Permission.MODIFY);
      try
      {
        client.upload(relativePath.toString(), inputStream);
      }
      catch (IOException ex)
      {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getApiTenant(), client.getApiUser(), "uploadWithClient",
                                     client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

  /**
   * Create a directory at provided path.
   * Intermediate directories in the path will be created as necessary.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @throws ForbiddenException - user not authorized
   */
  @Override
  public void mkdir(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
          throws WebApplicationException
  {
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.MODIFY);
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      mkdir(client, path);
    }
    catch (IOException | ServiceException ex)
    {
      throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "mkdir", sysId, path,
                                        ex.getMessage()), ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Create a directory at provided path using provided client.
   * Intermediate directories in the path will be created as necessary.
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws ForbiddenException - user not authorized
   */
  @Override
  public void mkdir(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException, ForbiddenException
  {
    Path relativePath = PathUtils.getRelativePath(path);
    LibUtils.checkPermitted(permsService, client.getApiTenant(), client.getApiUser(), client.getSystemId(),
                            relativePath, Permission.MODIFY);
    try
    {
      client.mkdir(relativePath.toString());
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getApiTenant(), client.getApiUser(), "mkdirWithClient",
                                   client.getSystemId(), path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Move or copy a file or directory
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param srcPath - source path on system relative to system rootDir
   * @param dstPath - destination path on system relative to system rootDir
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void moveOrCopy(@NotNull ResourceRequestUser rUser, @NotNull MoveCopyOperation op, @NotNull TapisSystem sys,
                           String srcPath, String dstPath)
            throws WebApplicationException
    {
      String opName = op.name().toLowerCase();
      String apiTenant = rUser.getOboTenantId();
      String apiUser = rUser.getOboUserId();
      String sysId = sys.getId();
      Path srcRelativePath = PathUtils.getRelativePath(srcPath);
      Path dstRelativePath = PathUtils.getRelativePath(dstPath);
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        // Check the source and destination both have MODIFY perm
        LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, srcRelativePath, Permission.MODIFY);
        LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, dstRelativePath, Permission.MODIFY);
        client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
        client.reserve();
        moveOrCopy(client, op, srcPath, dstPath);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", apiTenant, apiUser, opName, sysId, srcPath, ex.getMessage());
        throw new WebApplicationException(msg, ex);
      }
      finally
      {
        if (client != null) client.release();
      }
    }

  /**
   * Move or copy a file or directory using provided client
   *
   * @param client remote data client to use
   * @param srcPath - source path on system relative to system rootDir
   * @param dstPath - destination path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  @Override
  public void moveOrCopy(@NotNull IRemoteDataClient client,  @NotNull MoveCopyOperation op, String srcPath, String dstPath)
          throws ServiceException
  {
    String opName = op.name().toLowerCase();
    String apiTenant = client.getApiTenant();
    String apiUser = client.getApiUser();
    String sysId = client.getSystemId();
    Path srcRelativePath = PathUtils.getRelativePath(srcPath);
    Path dstRelativePath = PathUtils.getRelativePath(dstPath);
    // Check the source and destination permissions
    Permission srcPerm = Permission.READ;
    if (op.equals(MoveCopyOperation.MOVE)) srcPerm = Permission.MODIFY;
    LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, srcRelativePath, srcPerm);
    LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, dstRelativePath, Permission.MODIFY);
    try
    {
      // Get file info here so we can do some basic checks independent of the system type
      FileInfo srcFileInfo = client.getFileInfo(srcPath);
      // Make sure srcPath exists
      if (srcFileInfo == null)
      {
        throw new NotFoundException(LibUtils.getMsg("FILES_PATH_NOT_FOUND", apiTenant, apiUser, sysId, srcPath));
      }
      // If srcPath and dstPath are the same then throw an exception.
      if (srcRelativePath.equals(dstRelativePath))
      {
        throw new NotFoundException(LibUtils.getMsg("FILES_SRC_DST_SAME", apiTenant, apiUser, sysId, srcPath));
      }

      // Perform the operation
      if (op.equals(MoveCopyOperation.MOVE))
      {
        client.move(srcRelativePath.toString(), dstRelativePath.toString());
        // Update permissions in the SK
        permsService.replacePathPrefix(apiTenant, apiUser, sysId, srcRelativePath.toString(), dstRelativePath.toString());
      }
      else
      {
        client.copy(srcRelativePath.toString(), dstRelativePath.toString());
      }
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", apiTenant, apiUser, String.format("%sWithClient",opName), sysId,
                                   srcPath, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Delete a file or directory
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void delete(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
            throws WebApplicationException
    {
      String apiTenant = rUser.getOboTenantId();
      String apiUser = rUser.getOboUserId();
      String sysId = sys.getId();
      Path relativePath = PathUtils.getRelativePath(path);
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.MODIFY);
        client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
        client.reserve();
        delete(client, path);
      }
      catch (IOException | ServiceException ex)
      {
        throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "delete", sysId, path,
                                          ex.getMessage()), ex);
      }
      finally
      {
        if (client != null) client.release();
      }
    }

  /**
   * Delete a file or directory using provided client
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  @Override
  public void delete(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException
  {
    Path relativePath = PathUtils.getRelativePath(path);
    LibUtils.checkPermitted(permsService, client.getApiTenant(), client.getApiUser(), client.getSystemId(),
                            relativePath, Permission.MODIFY);
    try
    {
      client.delete(relativePath.toString());
      permsService.removePathPermissionFromAllRoles(client.getApiTenant(), client.getApiUser(), client.getSystemId(),
                                                    relativePath.toString());
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getApiTenant(), client.getApiUser(), "deleteWithClient",
                                   client.getSystemId(), path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  // ----------------------------------------------------------------------------------------------------
  // ------------- Support for GetContent
  // ----------------------------------------------------------------------------------------------------

  /**
   * Create StreamingOutput for downloading a zipped up directory
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path to download
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getZipStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
      try
      {
        getZip(rUser, output, sys, path);
      }
      catch (NotFoundException | ForbiddenException ex)
      {
        throw ex;
      }
      catch (Exception e)
      {
        throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), path, e.getMessage()), e);
      }
    };
    return outStream;
  }

  /**
   * Create StreamingOutput for downloading a range of bytes
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - file to download
   * @param range - optional range for bytes to send
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getByteRangeStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                            @NotNull String path, @NotNull HeaderByteRange range)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
    InputStream stream = null;
    try
    {
      stream = getByteRange(rUser, sys, path, range.getMin(), range.getMax());
      stream.transferTo(output);
    }
    catch (NotFoundException | ForbiddenException ex)
    {
      throw ex;
    }
    catch (Exception e)
    {
      throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), path, e.getMessage()), e);
    }
    finally
    {
      IOUtils.closeQuietly(stream);
    }
    };
    return outStream;
  }

  /**
   * Create StreamingOutput for downloading paginated blocks of bytes
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - file to download
   * @param startPage - Send 1k of UTF-8 encoded string back starting at specified block,
                        e.g. more=2 to start at 2nd block
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getPagedStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                        @NotNull String path, @NotNull Long startPage)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getPaginatedBytes(rUser, sys, path, startPage);
        stream.transferTo(output);
      }
      catch (NotFoundException | ForbiddenException ex)
      {
        throw ex;
      }
      catch (Exception e)
      {
        throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), path, e.getMessage()), e);
      }
      finally
      {
        IOUtils.closeQuietly(stream);
      }
    };
    return outStream;
  }

  /**
   * Create StreamingOutput for downloading a file
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - file to download
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getFullStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getAllBytes(rUser, sys, path);
        stream.transferTo(output);
      }
      catch (NotFoundException | ForbiddenException ex)
      {
        throw ex;
      }
      catch (Exception e)
      {
        throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), path, e.getMessage()), e);
      }
      finally
      {
        IOUtils.closeQuietly(stream);
      }
    };
    return outStream;
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Create a response and use it to start a zipped stream
   */
//  private void sendZip(String path, AsyncResponse asyncResponse)
//          throws NotFoundException, ServiceException, IOException
//  {
//    java.nio.file.Path filepath = Paths.get(path);
//    String filename = filepath.getFileName().toString();
//    StreamingOutput outStream = output ->
//    {
////      try {
////      getZip(client, output, path);
////      getZip(output, path);
////      } catch (Exception e) {
////        throw new WebApplicationException(Utils.getMsgAuth("FILES_CONT_ZIP_ERR", user, systemId, path), e);
////      }
//    };
//    String newName = FilenameUtils.removeExtension(filename) + ".zip";
//    String disposition = String.format("attachment; filename=%s", newName);
//    Response resp =  Response.ok(outStream, MediaType.APPLICATION_OCTET_STREAM)
//            .header("content-disposition", disposition)
//            .build();
//    asyncResponse.resume(resp);
//  }

  /**
   * Stream all content from object at path
   *
   * In order to have the method auto disconnect the client, we have to copy the original InputStream from the client
   * to another InputStream or else the finally block immediately disconnects.
   *
   * @param path - path on system relative to system rootDir
   * @return InputStream
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  InputStream getAllBytes(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
          throws ServiceException
  {
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.READ);
    // Get a remoteDataClient to stream contents
    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return client.getStream(relativePath.toString());
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getStream", sysId, path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Generate a stream for a range of bytes.
   *
   * @param path - path on system relative to system rootDir
   * @throws ServiceException general service error
   * @throws ForbiddenException user not authorized
   */
  InputStream getByteRange(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                                   long startByte, long count)
          throws ServiceException
  {
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.READ);
    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return client.getBytesByRange(path, startByte, count);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getBytes", sysId, path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Stream content from object at path with support for pagination
   *
   * @param path - path on system relative to system rootDir
   * @return InputStream
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  private InputStream getPaginatedBytes(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                        @NotNull String path, long startPAge)
          throws ServiceException
  {
    long startByte = (startPAge - 1) * 1024;
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.READ);
    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return client.getBytesByRange(path, startByte, startByte + 1023);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getPaginatedBytes", sysId, path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Generate a streaming zip archive of a target path.
   *
   * @param outputStream Stream receiving zip contents
   * @param path - path on system relative to system rootDir
   * @throws ServiceException general service error
   * @throws ForbiddenException user not authorized
   */
  void getZip(@NotNull ResourceRequestUser rUser, @NotNull OutputStream outputStream, @NotNull TapisSystem sys,
                      @NotNull String path)
          throws ServiceException
  {
    String apiTenant = rUser.getOboTenantId();
    String apiUser = rUser.getOboUserId();
    String sysId = sys.getId();
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    LibUtils.checkPermitted(permsService, apiTenant, apiUser, sysId, relativePath, Permission.READ);

    String cleanedPath = FilenameUtils.normalize(path);
    cleanedPath = StringUtils.removeStart(cleanedPath, "/");
    if (StringUtils.isEmpty(cleanedPath)) cleanedPath = "/";

    IRemoteDataClient client = null;
    try (ZipOutputStream zipStream = new ZipOutputStream(outputStream))
    {
      // Get a remoteDataClient to do the listing and stream contents
      client = remoteDataClientFactory.getRemoteDataClient(apiTenant, apiUser, sys, sys.getEffectiveUserId());
      client.reserve();
      // Step through a recursive listing up to some max depth
      List<FileInfo> listing = lsRecursive(client, path, MAX_RECURSION);
      for (FileInfo fileInfo : listing)
      {
        // Always add an entry for a dir to be sure empty directories are included
        if (fileInfo.isDir())
        {
          String tmpPath = StringUtils.removeStart(fileInfo.getPath(), "/");
          Path pth = Paths.get(cleanedPath).relativize(Paths.get(tmpPath));
          ZipEntry entry = new ZipEntry(StringUtils.appendIfMissing(pth.toString(), "/"));
          zipStream.putNextEntry(entry);
          zipStream.closeEntry();
        }
        else
        {
          try (InputStream inputStream = getAllBytes(rUser, sys, fileInfo.getPath()))
          {
            String tmpPath = StringUtils.removeStart(fileInfo.getPath(), "/");
            Path pth = Paths.get(cleanedPath).relativize(Paths.get(tmpPath));
            ZipEntry entry = new ZipEntry(pth.toString());
            zipStream.putNextEntry(entry);
            inputStream.transferTo(zipStream);
            zipStream.closeEntry();
          }
        }
      }
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getZip", sysId, path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Recursive method to build up list of files at a path
   * @param client remote data client to use
   * @param basePath - path on system relative to system rootDir
   * @param listing - collection of FileInfo objects being used to build up list
   * @param depth - depth currently being listed
   * @param maxDepth - maximum depth for recursion
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  private void listDirectoryRecurse(@NotNull IRemoteDataClient client, String basePath, List<FileInfo> listing,
                                    int depth, int maxDepth)
          throws ServiceException
  {
    List<FileInfo> currentListing = ls(client, basePath, MAX_LISTING_SIZE, 0);
    listing.addAll(currentListing);
    for (FileInfo fileInfo: currentListing)
    {
      if (fileInfo.isDir() && depth < maxDepth)
      {
        depth++;
        listDirectoryRecurse(client, fileInfo.getPath(), listing, depth, maxDepth);
      }
    }
  }
}
