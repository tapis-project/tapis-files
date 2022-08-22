package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
public class FileOpsService
{
  public static final int MAX_LISTING_SIZE = 1000;

  public enum MoveCopyOperation {MOVE, COPY}

  private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_FILES;
  // 0=systemId, 1=path, 2=tenant
  private final String TAPIS_FILES_URL_FORMAT = "tapis://{0}/{1}?tenant={2}";
  private static final int MAX_RECURSION = 20;

  private static final String SYSTEMS_SERVICE = TapisConstants.SERVICE_NAME_SYSTEMS;
  private static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  private static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;

  public static final Set<String> SVCLIST_IMPERSONATE = new HashSet<>(Set.of(JOBS_SERVICE));
  private static final Set<String> SVCLIST_SHAREDAPPCTX = new HashSet<>(Set.of(JOBS_SERVICE));

  // **************** Inject Services using HK2 ****************
  @Inject
  FilePermsService permsService;
  @Inject
  FileShareService shareService;
  @Inject
  RemoteDataClientFactory remoteDataClientFactory;
  @Inject
  ServiceContext serviceContext;

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
   */
  public void initService(String siteId1, String siteAdminTenantId1, String svcPassword) throws TapisException, TapisClientException
  {
    // Initialize service context and site info
    siteId = siteId1;
    siteAdminTenantId = siteAdminTenantId1;
    serviceContext.initServiceJWT(siteId, APPS_SERVICE, svcPassword);
    // Make sure DB is present and updated using flyway
    migrateDB();
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
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> ls(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                           long limit, long offset, String impersonationId)
          throws WebApplicationException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      // Check for READ permission or share
      checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);

      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return ls(client, path, limit, offset);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, "ls", sysId, path, ex.getMessage());
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
  public List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path, long limit, long offset)
          throws ServiceException
  {
    // Get path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(path).toString();
    try
    {
      List<FileInfo> listing = client.ls(relPathStr, limit, offset);
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
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "lsWithClient",
                                   client.getSystemId(), relPathStr, ex.getMessage());
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
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public List<FileInfo> lsRecursive(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                      @NotNull String path, int depth, String impersonationId)
            throws WebApplicationException
    {
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      String sysId = sys.getId();
      // Get path relative to system rootDir and protect against ../..
      Path relativePath = PathUtils.getRelativePath(path);
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        // Check for READ permission
        checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);

        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
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
  public void upload(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                     @NotNull InputStream inStrm)
          throws WebApplicationException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    String relPathStr = PathUtils.getRelativePath(path).toString();
    try
    {
      LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, path, Permission.MODIFY);
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
      client.reserve();
      upload(client, relPathStr, inStrm);
    }
    catch (IOException | ServiceException ex)
    {
      throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "upload", sysId, relPathStr,
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
    public void upload(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull InputStream inputStream)
            throws ServiceException
    {
      LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                              path, Permission.MODIFY);
      // Get path relative to system rootDir and protect against ../..
      String relPathStr = PathUtils.getRelativePath(path).toString();
      try
      {
        client.upload(relPathStr, inputStream);
      }
      catch (IOException ex)
      {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "uploadWithClient",
                                     client.getSystemId(), relPathStr, ex.getMessage());
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
   * @param sharedAppCtx - Indicates that request is part of a shared app context.
   * @throws ForbiddenException - user not authorized
   */
  public void mkdir(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                    boolean sharedAppCtx)
          throws WebApplicationException
  {
    String opName = "mkdir";
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();

    // If sharedAppCtx set confirm that it is allowed
    if (sharedAppCtx) checkSharedAppCtxAllowed(rUser, opName, sysId, path);

    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      // If not skipping auth then check auth
      if (!sharedAppCtx) LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, path, Permission.MODIFY);
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
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
  public void mkdir(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException, ForbiddenException
  {
    LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                            path, Permission.MODIFY);
    // Get path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(path).toString();
    try
    {
      client.mkdir(relPathStr);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "mkdirWithClient",
                                   client.getSystemId(), relPathStr, ex.getMessage());
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
    public void moveOrCopy(@NotNull ResourceRequestUser rUser, @NotNull MoveCopyOperation op, @NotNull TapisSystem sys,
                           String srcPath, String dstPath)
            throws WebApplicationException
    {
      String opName = op.name().toLowerCase();
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      String sysId = sys.getId();
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        // Check the source and destination both have MODIFY perm
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, srcPath, Permission.MODIFY);
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, dstPath, Permission.MODIFY);
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
        client.reserve();
        moveOrCopy(client, op, srcPath, dstPath);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, srcPath, ex.getMessage());
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
  public void moveOrCopy(@NotNull IRemoteDataClient client,  @NotNull MoveCopyOperation op, String srcPath, String dstPath)
          throws ServiceException
  {
    String opName = op.name().toLowerCase();
    String oboTenant = client.getOboTenant();
    String oboUser = client.getOboUser();
    String sysId = client.getSystemId();
    // Check the source and destination permissions
    Permission srcPerm = Permission.READ;
    if (op.equals(MoveCopyOperation.MOVE)) srcPerm = Permission.MODIFY;
    LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, srcPath, srcPerm);
    LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, dstPath, Permission.MODIFY);
    // Get paths relative to system rootDir and protect against ../..
    String srcRelPathStr = PathUtils.getRelativePath(srcPath).toString();
    String dstRelPathStr = PathUtils.getRelativePath(dstPath).toString();
    try
    {
      // Get file info here, so we can do some basic checks independent of the system type
      FileInfo srcFileInfo = client.getFileInfo(srcRelPathStr);
      // Make sure srcPath exists
      if (srcFileInfo == null)
      {
        throw new NotFoundException(LibUtils.getMsg("FILES_PATH_NOT_FOUND", oboTenant, oboUser, sysId, srcRelPathStr));
      }
      // If srcPath and dstPath are the same then throw an exception.
      if (srcRelPathStr.equals(dstRelPathStr))
      {
        throw new NotFoundException(LibUtils.getMsg("FILES_SRC_DST_SAME", oboTenant, oboUser, sysId, srcRelPathStr));
      }

      // Perform the operation
      if (op.equals(MoveCopyOperation.MOVE))
      {
        client.move(srcRelPathStr, dstRelPathStr);
        // Update permissions in the SK
        permsService.replacePathPrefix(oboTenant, oboUser, sysId, srcRelPathStr, dstRelPathStr);
      }
      else
      {
        client.copy(srcRelPathStr, dstRelPathStr);
      }
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, String.format("%sWithClient",opName), sysId,
                                   srcPath, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Delete a file or directory. Remove all permissions and shares from SK
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public void delete(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
            throws WebApplicationException
    {
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      String sysId = sys.getId();
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, path, Permission.MODIFY);
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
        client.reserve();
        // Delete files and permissions
        delete(client, path);
        // Remove shares with recurse=true
        shareService.removeAllSharesForPath(rUser, sysId, path, true);
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
   * Delete a file or directory using provided client. Remove all permissions and shares from SK
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  public void delete(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException
  {
    LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                            path, Permission.MODIFY);
    // Get path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(path).toString();
    try
    {
      client.delete(relPathStr);
      permsService.removePathPermissionFromAllRoles(client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                                    relPathStr);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "deleteWithClient",
                                   client.getSystemId(), relPathStr, ex.getMessage());
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
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getZipStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                      @NotNull String path, String impersonationId)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
      try
      {
        getZip(rUser, output, sys, path, impersonationId);
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
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getByteRangeStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                            @NotNull String path, @NotNull HeaderByteRange range,String impersonationId)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
    InputStream stream = null;
    try
    {
      stream = getByteRange(rUser, sys, path, range.getMin(), range.getMax(), impersonationId);
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
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getPagedStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                        @NotNull String path, @NotNull Long startPage, String impersonationId)
          throws WebApplicationException
  {
    // Make sure user has permission for this path
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    try
    {
      checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);
    }
    catch (ServiceException e)
    {
      throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), path, e.getMessage()), e);
    }

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
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getFullStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                       @NotNull String path, String impersonationId)
          throws WebApplicationException
  {
    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getAllBytes(rUser, sys, path, impersonationId);
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
  InputStream getAllBytes(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path, String impersonationId)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);

    // Get a remoteDataClient to stream contents
    IRemoteDataClient client = null;
    String relPathStr = relativePath.toString();
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return client.getStream(relPathStr);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getStream", sysId, relPathStr, ex.getMessage());
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
                                   long startByte, long count, String impersonationId)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);

    IRemoteDataClient client = null;
    String relPathStr = relativePath.toString();
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return client.getBytesByRange(relPathStr, startByte, count);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getBytes", sysId, relPathStr, ex.getMessage());
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
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Make sure user has permission for this path
    LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, path, Permission.READ);
    // Get path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(path).toString();
    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
      client.reserve();
      return client.getBytesByRange(relPathStr, startByte, startByte + 1023);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getPaginatedBytes", sysId, relPathStr, ex.getMessage());
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
              @NotNull String path, String impersonationId)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    // Make sure user has permission for this path
    checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);

    String cleanedPath = FilenameUtils.normalize(path);
    cleanedPath = StringUtils.removeStart(cleanedPath, "/");
    if (StringUtils.isEmpty(cleanedPath)) cleanedPath = "/";

    IRemoteDataClient client = null;
    try (ZipOutputStream zipStream = new ZipOutputStream(outputStream))
    {
      // Get a remoteDataClient to do the listing and stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, sys.getEffectiveUserId());
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
          try (InputStream inputStream = getAllBytes(rUser, sys, fileInfo.getPath(), impersonationId))
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

  /**
   * Confirm that caller or impersonationId has READ permission or share on path
   * If it is allowed due to READ permission then return null
   * If it is allowed due to a share then return the UserShareInfo which contains the grantor.
   * To use impersonationId it must be a service request from a service allowed to impersonate.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - system containing path
   * @param path - path to the file, dir or object
   * @throws ForbiddenException - oboUserId not authorized to perform operation
   */
//  private UserShareInfo checkAuthForReadOrShare(ResourceRequestUser rUser, TapisSystem system, Path path, String impersonationId)
  private void checkAuthForReadOrShare(ResourceRequestUser rUser, TapisSystem system, Path path, String impersonationId)
          throws ForbiddenException, ServiceException
  {
    String systemId = system.getId();
    String pathStr = path.toString();

    // To impersonate must be a service request from an allowed service.
    if (!StringUtils.isBlank(impersonationId))
    {
      // If a service request the username will be the service name. E.g. systems, jobs, streams, etc
      String svcName = rUser.getJwtUserId();
      if (!rUser.isServiceRequest() || !SVCLIST_IMPERSONATE.contains(svcName))
      {
        String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_IMPERSONATE", rUser, systemId, pathStr, impersonationId);
        throw new ForbiddenException(msg);
      }
      // An allowed service is impersonating, log it
      log.info(LibUtils.getMsgAuthR("FILES_AUTH_IMPERSONATE", rUser, systemId, pathStr, impersonationId));
    }

    // Finally, check for READ perm or share using oboUser or impersonationId
    // Certain services are allowed to impersonate an OBO user for the purposes of authorization
    //   and effectiveUserId resolution.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;

    // If user has READ permission then return
    //  else if shared then return
    if (permsService.isPermitted(rUser.getOboTenantId(), oboOrImpersonatedUser, systemId, pathStr, Permission.READ))
    {
      return;
//      return null;
    }
    else
    {
      try
      {
        if (shareService.isSharedWithUser(rUser, system, pathStr, oboOrImpersonatedUser)) return;
//        UserShareInfo si =  shareService.isSharedWithUser(rUser, system, pathStr, oboOrImpersonatedUser);
//        if (si != null) return si;
      }
      catch (TapisClientException e)
      {
        String msg = LibUtils.getMsgAuthR("FILES_SHARE_GET_ERR", rUser, systemId, pathStr, e.getMessage());
        log.error(msg, e);
        throw new ServiceException(msg, e);
      }
    }
    // No READ or share, throw exception
    String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", rUser.getOboTenantId(), oboOrImpersonatedUser, systemId,
                                 pathStr, Permission.READ);
    throw new ForbiddenException(msg);
  }

  /**
   * Confirm that caller is allowed to set sharedAppCtx.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param opName - operation name
   * @param sysId - name of the system
   * @param pathStr - path involved in operation
   * @throws ForbiddenException - user not authorized to perform operation
   */
  private void checkSharedAppCtxAllowed(ResourceRequestUser rUser, String opName, String sysId, String pathStr)
          throws ForbiddenException
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_SHAREDAPPCTX.contains(svcName))
    {
      String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_SHAREDAPPCTX", rUser, opName, sysId, pathStr);
      throw new ForbiddenException(msg);
    }
    // An allowed service is skipping auth, log it
    log.trace(LibUtils.getMsgAuthR("FILES_AUTH_SHAREDAPPCTX", rUser, opName, sysId, pathStr));
  }


  /*
   * Use datasource from Hikari connection pool to execute the flyway migration.
   */
  private void migrateDB()
  {
    Flyway flyway = Flyway.configure().dataSource(HikariConnectionPool.getDataSource()).load();
    // TODO remove workaround if possible. Figure out how to deploy X.Y.Z-SNAPSHOT repeatedly.
    // Use repair as workaround to avoid checksum error during develop/deploy of SNAPSHOT versions when it is not
    // a true migration.
    flyway.repair();
    flyway.migrate();
  }
}
