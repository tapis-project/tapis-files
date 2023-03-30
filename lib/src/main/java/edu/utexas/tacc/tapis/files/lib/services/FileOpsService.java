package edu.utexas.tacc.tapis.files.lib.services;

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
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import static edu.utexas.tacc.tapis.shared.uri.TapisUrl.TAPIS_PROTOCOL_PREFIX;

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
  public static final int MAX_RECURSION = 20;

  public enum MoveCopyOperation {MOVE, COPY}

  private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_FILES;
  // 0=systemId, 1=path, 2=tenant
  private final String TAPIS_FILES_URL_FORMAT = String.format("%s{0}/{1}?tenant={2}", TAPIS_PROTOCOL_PREFIX);

  private static final String SYSTEMS_SERVICE = TapisConstants.SERVICE_NAME_SYSTEMS;
  private static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  private static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;

  public static final Set<String> SVCLIST_IMPERSONATE = new HashSet<>(Set.of(JOBS_SERVICE));
  public static final Set<String> SVCLIST_SHAREDCTX = new HashSet<>(Set.of(JOBS_SERVICE));

  // **************** Inject Services using HK2 ****************
  @Inject
  FilePermsService permsService;
  @Inject
  FileShareService shareService;
  @Inject
  RemoteDataClientFactory remoteDataClientFactory;
  @Inject
  ServiceContext serviceContext;
  @Inject
  SystemsCache systemsCache;
  @Inject
  SystemsCacheNoAuth systemsCacheNoAuth;

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
    serviceContext.initServiceJWT(siteId, APPS_SERVICE, svcPassword);
    FileShareService.setSiteAdminTenantId(siteAdminTenantId1);
    FilePermsService.setSiteAdminTenantId(siteAdminTenantId1);
    // Make sure DB is present and updated using flyway
    migrateDB();
  }

  // ----------------------------------------------------------------------------------------------------
  // ------------- Support for FileOps
  // ----------------------------------------------------------------------------------------------------
  /**
   * Non-recursive list of files at path using given limit and offset
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sysId - System
   * @param pathStr - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> ls(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                           long limit, long offset, String impersonationId, String sharedCtxGrantor)
          throws WebApplicationException
  {
    String opName = "ls";
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.READ,
                                                           impersonationId, sharedCtxGrantor);

    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      // Get the connection and increment the reservation count
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, impersonationId, sharedCtxGrantor);
      client.reserve();
      return ls(client, relPathStr, limit, offset);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
    finally
    {
      // Release the connection
      if (client != null) client.release();
    }
  }

  /**
   * List files at path using provided client and given limit and offset
   * NOTE: This method does not check permissions. Callers should check first
   * @param client - Remote data client
   * @param pathStr - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String pathStr, long limit, long offset)
          throws ServiceException
  {
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();
    try
    {
      List<FileInfo> listing = client.ls(relPathStr, limit, offset);
      listing.forEach(f ->
        {
          f.setUrl(PathUtils.getTapisUrlFromPath(f.getPath(), client.getSystemId()));
          // Ensure there is no leading slash
          f.setPath(StringUtils.removeStart(f.getPath(), "/"));
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
   * Recursive list of files at path. Max possible depth = MAX_RECURSION(20)
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sysId - System
   * @param pathStr - path on system relative to system rootDir
   * @param depth - maximum depth for recursion
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public List<FileInfo> lsRecursive(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                                      int depth, String impersonationId, String sharedCtxGrantor)
            throws WebApplicationException
    {
      String opName = "lsRecursive";
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      // Get normalized path relative to system rootDir and protect against ../..
      String relPathStr = PathUtils.getRelativePath(pathStr).toString();

      // Fetch system with credentials including auth checks for system and path
      TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                             opName, sysId, relPathStr, Permission.READ,
                                                             impersonationId, sharedCtxGrantor);

      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        // Get the connection and increment the reservation count
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
        client.reserve();
        return lsRecursive(client, relPathStr, depth);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, sysId, relPathStr, ex.getMessage());
        log.error(msg, ex);
        throw new WebApplicationException(msg, ex);
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
   * @param relPathStr - normalized path on system relative to system rootDir
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String relPathStr, int depth)
          throws ServiceException
  {
    List<FileInfo> listing = new ArrayList<>();
    // Make the call that does recursion
    listDirectoryRecurse(client, relPathStr, listing, 0, Math.min(depth, MAX_RECURSION));
    return listing;
  }

  /**
   * Get FileInfo for a path
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sysId - System
   * @param pathStr - path on system relative to system rootDir
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return FileInfo or null if not found
   * @throws NotFoundException - requested path not found
   */
  public FileInfo getFileInfo(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                              String impersonationId, String sharedCtxGrantor)
          throws WebApplicationException
  {
    String opName = "getFileInfo";
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.READ,
                                                           impersonationId, sharedCtxGrantor);

    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
//TODO - SHARED      // If not skipping auth then check for READ/MODIFY permission or share
//TODO - SHARED//TODO REMOVE      if (!sharedCtx) checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);
//TODO - SHARED      // Check for READ/MODIFY permission or share
//TODO - SHARED      checkAuthForPath(rUser, sys, relativePath, Permission.READ, impersonationId, sharedCtxGrantor);
      // Get the connection and increment the reservation count
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      client.reserve();
      return client.getFileInfo(pathStr);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
    finally
    {
      // Release the connection
      if (client != null) client.release();
    }
  }

  /**
   * Upload a file
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param pathStr - path on system relative to system rootDir
   * @param inStrm  data stream to be used when creating file
   * @throws ForbiddenException - user not authorized
   */
  public void upload(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String pathStr,
                     @NotNull InputStream inStrm)
          throws WebApplicationException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, pathStr, Permission.MODIFY);
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      client.reserve();
      upload(client, relPathStr, inStrm);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "upload", sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  /**
   * Upload a file using provided client
   * @param client remote data client to use
   * @param relPathStr - normalized path on system relative to system rootDir
   * @param inputStream  data stream to be used when creating file
   * @throws ServiceException - general error
   * @throws ForbiddenException - user not authorized
   */
    public void upload(@NotNull IRemoteDataClient client, @NotNull String relPathStr, @NotNull InputStream inputStream)
            throws ServiceException
    {
      LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                              relPathStr, Permission.MODIFY);

      // Make sure this is not an attempt to upload to "/". Check "/" first because "/" resolves to empty string
      //   for relative path
      if ("/".equals(relPathStr))
      {
        String msg = LibUtils.getMsg("FILES_ERR_SLASH_PATH", client.getOboTenant(), client.getOboUser(), "upload",
                client.getSystemId());
        throw new ServiceException(msg);
      }

      // Make sure this is not an attempt to upload to an empty string
      if (StringUtils.isBlank(relPathStr))
      {
        String msg = LibUtils.getMsg("FILES_ERR_EMPTY_PATH", client.getOboTenant(), client.getOboUser(), "upload",
                                     client.getSystemId());
        throw new ServiceException(msg);
      }
      try
      {
        // Make sure file does not already exist as a directory
        var fInfo = client.getFileInfo(relPathStr);
        if (fInfo != null && fInfo.isDir())
        {
          String msg = LibUtils.getMsg("FILES_ERR_UPLOAD_DIR", client.getOboTenant(), client.getOboUser(),
                                       client.getSystemId(), relPathStr);
          throw new ServiceException(msg);
        }
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
   * @param sysId - System
   * @param pathStr - path on system relative to system rootDir
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws ForbiddenException - user not authorized
   */
  public void mkdir(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                    String sharedCtxGrantor)
          throws WebApplicationException
  {
    // Trace the call
    log.debug(LibUtils.getMsgAuthR("FILES_OP_MKDIR", rUser, sysId, pathStr, sharedCtxGrantor));
    String opName = "mkdir";
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String impersonationIdNull = null;
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.MODIFY,
                                                           impersonationIdNull, sharedCtxGrantor);

    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
//TODO - SHARED      // If not skipping auth then check auth
//TODO - SHARED      if (!sharedCtx) LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, pathStr, Permission.MODIFY);

      // Get the connection and increment the reservation count
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, null, sharedCtxGrantor);
      client.reserve();
      mkdir(client, relPathStr);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, sysId, pathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
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
   * @param relPathStr - normalized path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws ForbiddenException - user not authorized
   */
  public void mkdir(@NotNull IRemoteDataClient client, @NotNull String relPathStr)
          throws ServiceException, ForbiddenException
  {
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
   * @param srcPathStr - source path on system relative to system rootDir
   * @param dstPathStr - destination path on system relative to system rootDir
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public void moveOrCopy(@NotNull ResourceRequestUser rUser, @NotNull MoveCopyOperation op, @NotNull TapisSystem sys,
                           String srcPathStr, String dstPathStr)
            throws WebApplicationException
    {
      String opName = op.name().toLowerCase();
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      String sysId = sys.getId();
      // Get normalized paths relative to system rootDir and protect against ../..
      String srcRelPathStr = PathUtils.getRelativePath(srcPathStr).toString();
      String dstRelPathStr = PathUtils.getRelativePath(dstPathStr).toString();

      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        // Check the source and destination both have MODIFY perm
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, srcRelPathStr, Permission.MODIFY);
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, dstRelPathStr, Permission.MODIFY);
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
        client.reserve();
        moveOrCopy(client, op, srcRelPathStr, dstRelPathStr);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, srcRelPathStr, ex.getMessage());
        log.error(msg, ex);
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
   * @param srcRelPathStr - normalized source path on system relative to system rootDir
   * @param dstRelPathStr - normalized destination path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  public void moveOrCopy(@NotNull IRemoteDataClient client,  @NotNull MoveCopyOperation op,
                         @NotNull String srcRelPathStr, @NotNull String dstRelPathStr)
          throws ServiceException
  {
    String opName = op.name().toLowerCase();
    String oboTenant = client.getOboTenant();
    String oboUser = client.getOboUser();
    String sysId = client.getSystemId();
    // Check permission for source. If Copy only need READ/MODIFY, if Move then must have MODIFY
    // TODO: What about a shared path? Cannot use checkForReadOrShared here without significant refactoring of code.
    if (op.equals(MoveCopyOperation.COPY))
    {
      LibUtils.checkPermittedReadOrModify(permsService, oboTenant, oboUser, sysId, srcRelPathStr);
    }
    else
    {
      LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, srcRelPathStr, Permission.MODIFY);
    }
    // Check permission for destination
    LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, dstRelPathStr, Permission.MODIFY);

    try
    {
      // Get file info here, so we can do some basic checks independent of the system type
      FileInfo srcFileInfo = client.getFileInfo(srcRelPathStr);
      // Make sure srcPath exists
      if (srcFileInfo == null)
      {
        String msg = LibUtils.getMsg("FILES_PATH_NOT_FOUND", oboTenant, oboUser, sysId, srcRelPathStr);
        log.warn(msg);
        throw new NotFoundException(msg);
      }
      // If srcPath and dstPath are the same then throw an exception.
      if (srcRelPathStr.equals(dstRelPathStr))
      {
        String msg = LibUtils.getMsg("FILES_SRC_DST_SAME", oboTenant, oboUser, sysId, srcRelPathStr);
        log.warn(msg);
        throw new NotFoundException(msg);
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
                                   srcRelPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Delete a file or directory. Remove all permissions and shares from SK
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param pathStr - path on system relative to system rootDir
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public void delete(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String pathStr)
            throws WebApplicationException
    {
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      String sysId = sys.getId();
      // Get normalized path relative to system rootDir and protect against ../..
      String relativePathStr = PathUtils.getRelativePath(pathStr).toString();
      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, sysId, relativePathStr, Permission.MODIFY);
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
        client.reserve();
        // Delete files and permissions
        delete(client, relativePathStr);
        // Remove shares with recurse=true
        shareService.removeAllSharesForPathWithoutAuth(oboTenant, sysId, relativePathStr, true);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "delete", sysId, relativePathStr, ex.getMessage());
        log.error(msg, ex);
        throw new WebApplicationException(msg, ex);
      }
      finally
      {
        if (client != null) client.release();
      }
    }

  /**
   * Delete a file or directory using provided client. Remove all permissions and shares from SK
   * @param client remote data client to use
   * @param relPathStr - normalized path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  public void delete(@NotNull IRemoteDataClient client, @NotNull String relPathStr) throws ServiceException
  {
    LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                            relPathStr, Permission.MODIFY);
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
   * @param sysId - System
   * @param pathStr - path to download
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getZipStream(@NotNull ResourceRequestUser rUser, @NotNull String sysId,
                                      @NotNull String pathStr, String impersonationId, String sharedCtxGrantor)
          throws WebApplicationException
  {
    String opName = "getZipStream";
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.READ,
                                                           impersonationId, sharedCtxGrantor);

    // If rootDir + path results in all files then reject
    String rootDir = sys.getRootDir();
    if ((StringUtils.isBlank(rootDir) || rootDir.equals("/")) &&
        (StringUtils.isBlank(relPathStr) || relPathStr.equals("/")))
    {
      String msg = LibUtils.getMsgAuthR("FILES_CONT_NOZIP", rUser, sysId, rootDir, pathStr);
      throw new BadRequestException(msg);
    }

    StreamingOutput outStream = output -> {
      try
      {
//TODO - SHARED
        getZip(rUser, output, sys, pathStr, impersonationId, sharedCtxGrantor);
      }
      catch (NotFoundException | ForbiddenException ex)
      {
        throw ex;
      }
      catch (Exception e)
      {
        String msg = LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sysId, relPathStr, e.getMessage());
        log.error(msg, e);
        throw new WebApplicationException(msg, e);
      }
    };
    return outStream;
  }

  /**
   * Create StreamingOutput for downloading a range of bytes
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sysId - System
   * @param pathStr - file to download
   * @param range - optional range for bytes to send
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getByteRangeStream(@NotNull ResourceRequestUser rUser, @NotNull String sysId,
                                            @NotNull String pathStr, @NotNull HeaderByteRange range,
                                            String impersonationId, String sharedCtxGrantor)
          throws WebApplicationException
  {
    String opName = "getByteRangeStream";
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.READ,
                                                           impersonationId, sharedCtxGrantor);

    StreamingOutput outStream = output -> {
    InputStream stream = null;
    try
    {
//TODO - SHARED
      stream = getByteRange(rUser, sys, path, range.getMin(), range.getMax(), impersonationId, sharedCtxGrantor);
      stream.transferTo(output);
    }
    catch (NotFoundException | ForbiddenException ex)
    {
      throw ex;
    }
    catch (Exception e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), pathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
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
   * @param sysId - System
   * @param pathStr - file to download
   * @param startPage - Send 1k of UTF-8 encoded string back starting at specified block,
                        e.g. more=2 to start at 2nd block
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getPagedStream(@NotNull ResourceRequestUser rUser, @NotNull String sysId,
                                        @NotNull String pathStr, @NotNull Long startPage, String impersonationId,
                                        String sharedCtxGrantor)
          throws WebApplicationException
  {
    String opName = "getPagedStream";
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.READ,
                                                           impersonationId, sharedCtxGrantor);

//TODO - SHARED    // If sharedCtx set, confirm that it is allowed (can throw ForbiddenException)
//TODO - SHARED    boolean sharedCtx = !StringUtils.isBlank(sharedCtxGrantor);
//TODO - SHARED    if (sharedCtx) checkSharedCtxAllowed(rUser, opName, sys.getId(), relativePath.toString(), sharedCtxGrantor);

    // Make sure user has permission for this path
//TODO - SHARED    try
//TODO - SHARED    {
      // If not skipping auth check, check for READ/MODIFY permission or share
//TODO - SHARED      if (!sharedCtx) checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);
//TODO      // Check for READ/MODIFY permission or share
//TODO      checkAuthForPath(rUser, sys, relativePath, Permission.READ, impersonationId, sharedCtxGrantor);
//TODO - SHARED    }
//TODO - SHARED    catch (ServiceException e)
//TODO - SHARED    {
//TODO - SHARED      String msg = LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), pathStr, e.getMessage());
//TODO - SHARED      log.error(msg, e);
//TODO - SHARED      throw new WebApplicationException(msg, e);
//TODO - SHARED    }

    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getPaginatedBytes(rUser, sys, relPathStr, startPage);
        stream.transferTo(output);
      }
//TODO - SHARED
      catch (NotFoundException | ForbiddenException ex)
      {
        throw ex;
      }
      catch (Exception e)
      {
        String msg = LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sysId, relPathStr, e.getMessage());
        log.error(msg, e);
        throw new WebApplicationException(msg, e);
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
   * @param sysId - System
   * @param pathStr - file to download
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws NotFoundException System or path not found
   */
  public StreamingOutput getFullStream(@NotNull ResourceRequestUser rUser, @NotNull String sysId,
                                       @NotNull String pathStr, String impersonationId, String sharedCtxGrantor)
          throws WebApplicationException
  {
    String opName = "getFullStream";
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, sysId, relPathStr, Permission.READ,
                                                           impersonationId, sharedCtxGrantor);
    // If sharedCtx set, confirm that it is allowed (can throw ForbiddenException)
//TODO - SHARED    boolean sharedCtx = !StringUtils.isBlank(sharedCtxGrantor);
//TODO - SHARED    if (sharedCtx) checkSharedCtxAllowed(rUser, opName, sys.getId(), relativePath.toString(), sharedCtxGrantor);

//TODO - SHARED    try
//TODO - SHARED    {
      // Check for READ/MODIFY permission or share
//TODO - SHARED      checkAuthForPath(rUser, sys, relativePath, Permission.READ, impersonationId, sharedCtxGrantor);
//TODO - SHARED    }
//TODO - SHARED    catch (ServiceException e)
//TODO - SHARED    {
//TODO - SHARED      String msg = LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sys.getId(), pathStr, e.getMessage());
//TODO - SHARED      log.error(msg, e);
//TODO - SHARED      throw new WebApplicationException(msg, e);
//TODO - SHARED    }

    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getAllBytes(rUser, sys, pathStr);
        stream.transferTo(output);
      }
//TODO - SHARED
        catch (NotFoundException | ForbiddenException ex)
      {
        throw ex;
      }
      catch (Exception e)
      {
        String msg = LibUtils.getMsgAuthR("FILES_CONT_ERR", rUser, sysId, relPathStr, e.getMessage());
        log.error(msg, e);
        throw new WebApplicationException(msg, e);
      }
      finally
      {
        IOUtils.closeQuietly(stream);
      }
    };
    return outStream;
  }

//TODO - SHARED
//// TODO remove once using sharedCtxGrantor?
  /**
   * Determine if file path is shared
   * NOTE: We do not check here that impersonation is allowed. The 2 methods that call this method later call
   *       checkAuthForReadOrShare which does the check
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param path - path on system relative to system rootDir
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @return true if shared else false
   */
  public boolean isPathShared(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                              String impersonationId) throws WebApplicationException
  {
    // Get path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(path).toString();
    // Certain services are allowed to impersonate an OBO user for the purposes of authorization
    //   and effectiveUserId resolution.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    try
    {
      if (shareService.isSharedWithUser(rUser, sys, relativePathStr, oboOrImpersonatedUser)) return true;
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_GET_ERR", rUser, sys.getId(), relativePathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
    return false;
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
////        String msg = Utils.getMsgAuth("FILES_CONT_ZIP_ERR", user, systemId, path);
////        log.error(msg, e);
////        throw new WebApplicationException(msg, e);
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
   * @param pathStr - path on system relative to system rootDir
   * @return InputStream
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  InputStream getAllBytes(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String pathStr)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get normalized path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(pathStr);

    // Get a remoteDataClient to stream contents
    IRemoteDataClient client = null;
    String relPathStr = relativePath.toString();
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      client.reserve();
      return client.getStream(relPathStr);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getAllBytes", sysId, relPathStr, ex.getMessage());
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
   * @param pathStr - path on system relative to system rootDir
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws ServiceException general service error
   * @throws ForbiddenException user not authorized
   */
//TODO - SHARED
  InputStream getByteRange(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String pathStr,
                           long startByte, long count, String impersonationId, String sharedCtxGrantor)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();
//TODO - SHARED
    // If not skipping auth check, check for READ/MODIFY permission or share
//TODO sharedCtxGrantor    if (!sharedAppCtx) checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);
    checkAuthForReadOrShare(rUser, sys, relativePath, impersonationId);
//TODO    // Check for READ/MODIFY permission or share
//TODO    checkAuthForPath(rUser, sys, relativePath, Permission.READ, impersonationId, sharedCtxGrantor);

    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      client.reserve();
      return client.getBytesByRange(relPathStr, startByte, count);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getByteRange", sysId, relPathStr, ex.getMessage());
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
   * @param relPathStr - path on system relative to system rootDir
   * @return InputStream
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
  private InputStream getPaginatedBytes(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys,
                                        @NotNull String relPathStr, long startPAge)
          throws ServiceException
  {
    long startByte = (startPAge - 1) * 1024;
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
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
   * @param pathStr - path on system relative to system rootDir
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws ServiceException general service error
   * @throws ForbiddenException user not authorized
   */
  void getZip(@NotNull ResourceRequestUser rUser, @NotNull OutputStream outputStream, @NotNull TapisSystem sys,
              @NotNull String pathStr, String impersonationId, String sharedCtxGrantor)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

//TODO - SHARED    // Check for READ/MODIFY permission or share
//TODO - SHARED    checkAuthForPath(rUser, sys, relativePath, Permission.READ, impersonationId, sharedCtxGrantor);

    String cleanedRelativePathString = FilenameUtils.normalize(pathStr);
    cleanedRelativePathString = StringUtils.prependIfMissing(cleanedRelativePathString, "/");

    IRemoteDataClient client = null;
    // Create an output stream and start adding to it
    try (ZipOutputStream zipStream = new ZipOutputStream(outputStream))
    {
      // Get a remoteDataClient to do the listing and stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      client.reserve();
      // Step through a recursive listing up to some max depth
      List<FileInfo> listing = lsRecursive(client, relPathStr, MAX_RECURSION);
      for (FileInfo fileInfo : listing)
      {
        // Build the path we will use for the zip entry
        // To relativize we need a leading slash
        Path fileInfoPath = Paths.get("/", fileInfo.getPath());
        Path zipRootPath = Paths.get("/", FilenameUtils.getPath(cleanedRelativePathString));
        Path currentPath = zipRootPath.relativize(fileInfoPath);

        // For final entry we do not want the leading slash
        String entryPath = StringUtils.removeStart(currentPath.toString(), "/");

        // Always add an entry for a dir to be sure empty directories are included
        if (fileInfo.isDir())
        {
          addDirectoryToZip(zipStream, entryPath);
        }
        else
        {
          try (InputStream inputStream = getAllBytes(rUser, sys, fileInfo.getPath()))
          {
            addFileToZip(zipStream, entryPath, inputStream);
          }
        }
      }
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getZip", sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    finally
    {
      if (client != null) client.release();
    }
  }

  private void addDirectoryToZip(ZipOutputStream zos, String directoryPath) throws IOException {
    // Since it is a dir we add a trailing slash
    directoryPath = StringUtils.appendIfMissing(directoryPath, "/");
    ZipEntry entry = new ZipEntry(directoryPath);
    zos.putNextEntry(entry);
    zos.closeEntry();
  }
  private void addFileToZip(ZipOutputStream zos, String filePath, InputStream inputStream) throws IOException {
    ZipEntry entry = new ZipEntry(filePath);
    zos.putNextEntry(entry);
    inputStream.transferTo(zos);
    zos.closeEntry();
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
    // If client is S3 we are done.
    if (SystemTypeEnum.S3.equals(client.getSystemType())) return;
    for (FileInfo fileInfo: currentListing)
    {
      if (fileInfo.isDir() && depth < maxDepth)
      {
        depth++;
        listDirectoryRecurse(client, fileInfo.getPath(), listing, depth, maxDepth);
      }
    }
  }

//TODO - SHARED
  /**
   * Confirm that caller or impersonationId has READ/MODIFY permission or share on path
   * To use impersonationId it must be a service request from a service allowed to impersonate.
   * NOTE: Consider implementing as follows:
   *   If it is allowed due to READ or MODIFY permission then return null
   *   If it is allowed due to a share then return the UserShareInfo which contains the grantor.
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
        log.warn(msg);
        throw new ForbiddenException(msg);
      }
      // An allowed service is impersonating, log it
      log.info(LibUtils.getMsgAuthR("FILES_AUTH_IMPERSONATE", rUser, systemId, pathStr, impersonationId));
    }

    // Finally, check for READ perm or share using oboUser or impersonationId
    // Certain services are allowed to impersonate an OBO user for the purposes of authorization
    //   and effectiveUserId resolution.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;

    // If requesting user is the system owner no need to check
    if (oboOrImpersonatedUser.equals(system.getOwner())) return;

    // If user has READ or MODIFY permission then return
    //  else if shared then return
    if (permsService.isPermitted(rUser.getOboTenantId(), oboOrImpersonatedUser, systemId, pathStr, Permission.READ) ||
        permsService.isPermitted(rUser.getOboTenantId(), oboOrImpersonatedUser, systemId, pathStr, Permission.MODIFY))
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
    // No READ, MODIFY or share, throw exception
    String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", rUser.getOboTenantId(), oboOrImpersonatedUser, systemId,
                                 pathStr, Permission.READ);
    log.warn(msg);
    throw new ForbiddenException(msg);
  }

//TODO - SHARED
  /**
   * Confirm that caller or impersonationId has READ and/or MODIFY permission or share on path
   * To use impersonationId it must be a service request from a service allowed to impersonate.
   * If READ perm passed in then can be READ or MODIFY. If MODIFY passed in must be MODIFY
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - system containing path
   * @param relativePath - normalized path to the file, dir or object. Relative to system rootDir
   * @param perm - required permission (READ or MODIFY)
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws ForbiddenException - oboUserId not authorized to perform operation
   */
  private void checkAuthForPath(ResourceRequestUser rUser, TapisSystem system, Path relativePath, Permission perm,
                                String impersonationId, String sharedCtxGrantor)
          throws ForbiddenException, ServiceException
  {
    String systemId = system.getId();
    String relativePathStr = relativePath.toString();
    String sysOwner = system.getOwner();
    String oboTenant = rUser.getOboTenantId();
    boolean modifyRequired = Permission.MODIFY.equals(perm);
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    boolean sharedCtx = !StringUtils.isBlank(sharedCtxGrantor);

    // To impersonate must be a service request from an allowed service.
    if (!StringUtils.isBlank(impersonationId))
    {
      // If a service request the username will be the service name. E.g. systems, jobs, streams, etc
      String svcName = rUser.getJwtUserId();
      if (!rUser.isServiceRequest() || !SVCLIST_IMPERSONATE.contains(svcName))
      {
        String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_IMPERSONATE", rUser, systemId, relativePathStr, impersonationId);
        log.warn(msg);
        throw new ForbiddenException(msg);
      }
      // An allowed service is impersonating, log it
      log.info(LibUtils.getMsgAuthR("FILES_AUTH_IMPERSONATE", rUser, systemId, relativePathStr, impersonationId));
    }
    // TODO REMOVE
    // TODO For now if sharedCtxGrantor set to anything then allow
    // TODO REMOVE
    if (sharedCtx) return;

    // If obo user is owner or in shared context and share grantor is owner then allow.
    if (oboOrImpersonatedUser.equals(sysOwner) || (sharedCtx && sharedCtxGrantor.equals(sysOwner))) return;

    // Check for fine-grained permission READ/MODIFY or MODIFY for obo user
    if (modifyRequired)
    {
      if (permsService.isPermitted(oboTenant, oboOrImpersonatedUser, systemId, relativePathStr, Permission.MODIFY)) return;
    }
    else
    {
      if (permsService.isPermitted(oboTenant, oboOrImpersonatedUser, systemId, relativePathStr, Permission.READ) ||
          permsService.isPermitted(oboTenant, oboOrImpersonatedUser, systemId, relativePathStr, Permission.MODIFY)) return;
    }

    // Check for fine-grained permission READ/MODIFY or MODIFY for share grantor
    if (sharedCtx && modifyRequired)
    {
      if (permsService.isPermitted(oboTenant, sharedCtxGrantor, systemId, relativePathStr, Permission.MODIFY)) return;
    }
    else if (sharedCtx)
    {
      if (permsService.isPermitted(oboTenant, sharedCtxGrantor, systemId, relativePathStr, Permission.READ) ||
          permsService.isPermitted(oboTenant, sharedCtxGrantor, systemId, relativePathStr, Permission.MODIFY)) return;
    }

// TODO sharedCtxGrantor
    // Check for share with obo user / impersonationId or shareGrantor
    // TODO sharing must now include share by priv READ or MODIFY
    //    Currently sharing only allows for READ. So check only for READ case.
    //    When sharing updated to share by privilege then update here.
    // TODO/TBD previously sharedCtx was bool and all auth checking turned off. Skip check for MODIFY for now?
    //          Let READ be enough?
    try
    {
      if (Permission.READ.equals(perm))
      {
        if (shareService.isSharedWithUser(rUser, system, relativePathStr, oboOrImpersonatedUser)) return;
        if (sharedCtx)
        {
          if (shareService.isSharedWithUser(rUser, system, relativePathStr, sharedCtxGrantor)) return;
        }
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_GET_ERR", rUser, systemId, relativePathStr, e.getMessage());
      log.error(msg, e);
      throw new ServiceException(msg, e);
    }

    // Nothing allowed the operation. Throw ForbiddenException
    String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", rUser.getOboTenantId(), oboOrImpersonatedUser, systemId,
                                 relativePathStr, perm);
    log.warn(msg);
    throw new ForbiddenException(msg);
  }


//TODO - SHARED REMOVE?
  /**
   * Confirm that caller is allowed to set sharedCtxGrantor.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param opName - operation name
   * @param sysId - name of the system
   * @param pathStr - path involved in operation, used for logging
   * @throws ForbiddenException - user not authorized to perform operation
   */
  private static void checkSharedCtxAllowed(ResourceRequestUser rUser, String opName, String sysId, String pathStr,
                                            String sharedCtxGrantor)
          throws ForbiddenException
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_SHAREDCTX.contains(svcName))
    {
      String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_SHAREDCTX", rUser, opName, sysId, pathStr, sharedCtxGrantor);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
    // An allowed service is skipping auth, log it
    log.debug(LibUtils.getMsgAuthR("FILES_AUTH_SHAREDCTX", rUser, opName, sysId, pathStr, sharedCtxGrantor));
  }

  /*
   * Use datasource from Hikari connection pool to execute the flyway migration.
   */
  private void migrateDB()
  {
    Flyway flyway = Flyway.configure().dataSource(HikariConnectionPool.getDataSource()).load();
    // Use repair as workaround to avoid checksum error during develop/deploy of SNAPSHOT versions when it is not
    // a true migration.
//    flyway.repair();
    flyway.migrate();
  }
}
