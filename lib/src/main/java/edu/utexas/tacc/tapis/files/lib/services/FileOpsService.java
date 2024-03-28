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

import edu.utexas.tacc.tapis.files.lib.clients.ISSHDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.SSHDataClient;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
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

  public enum MoveCopyOperation {MOVE, COPY, SERVICE_MOVE_DIRECTORY_CONTENTS, SERVICE_MOVE_FILE_OR_DIRECTORY}

  private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_FILES;

  // Some methods do not support impersonationId or sharedAppCtxGrantor
  private static final String impersonationIdNull = null;
  private static final String sharedCtxGrantorNull = null;

  // 0=systemId, 1=path, 2=tenant
  private final String TAPIS_FILES_URL_FORMAT = String.format("%s{0}/{1}?tenant={2}", TAPIS_PROTOCOL_PREFIX);

  private static final String SYSTEMS_SERVICE = TapisConstants.SERVICE_NAME_SYSTEMS;
  private static final String APPS_SERVICE = TapisConstants.SERVICE_NAME_APPS;
  private static final String JOBS_SERVICE = TapisConstants.SERVICE_NAME_JOBS;
  private static final String FILES_SERVICE = TapisConstants.SERVICE_NAME_FILES;

  public static final Set<String> SVCLIST_IMPERSONATE = new HashSet<>(Set.of(JOBS_SERVICE));
  public static final Set<String> SVCLIST_SHAREDCTX = new HashSet<>(Set.of(JOBS_SERVICE, FILES_SERVICE));

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
   * @param pattern - regex used to filter results.  Only results with file names that match the regex will be returned
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> ls(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                           long limit, long offset, String pattern, String impersonationId, String sharedCtxGrantor)
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
      return ls(client, relPathStr, limit, offset, pattern);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      // if our connection fails, invalidate the systems cache in the hopes that if we try again, we will
      // get fresh credentials.
      systemsCache.invalidateEntry(oboTenant, sysId, oboUser, impersonationId, sharedCtxGrantor);
      systemsCacheNoAuth.invalidateEntry(oboTenant, sysId, oboUser);
      throw new WebApplicationException(msg, ex);
    }
  }

  /**
   * List files at path using provided client and given limit and offset
   * NOTE: This method does not check permissions. Callers should check first
   * @param client - Remote data client
   * @param pathStr - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @param pattern - wildcard (glob) pattern or regex used to filter results.  Regex must be prefixed with "regex:".
   *                Only results with file names that match the regex will be returned
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String pathStr, long limit, long offset, String pattern)
          throws ServiceException
  {
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();
    try
    {
      List<FileInfo> listing = client.ls(relPathStr, limit, offset, pattern);
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
   * @param regex - regex used to filter results.  Only results with file names that match the regex will be returned
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return Collection of FileInfo objects
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public List<FileInfo> lsRecursive(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                                      int depth, String regex, String impersonationId, String sharedCtxGrantor)
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
        return lsRecursive(client, relPathStr, false, depth, regex);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, sysId, relPathStr, ex.getMessage());
        log.error(msg, ex);
        throw new WebApplicationException(msg, ex);
      }
    }

  /**
   * Recursive list files at path using provided client.
   * Max possible depth = MAX_RECURSION(20)
   * NOTE: This method does not check permissions. Callers should check first
   * @param client - Remote data client
   * @param relPathStr - normalized path on system relative to system rootDir
   * @param followLinks - if true, symlinks will be followed.  This really means that we will go look at the
   *                    file pointed to by the link to get it's attributes.  The path will not be updated to
   *                    the path of the file pointed to by the link - it just affects the attributes.
   * @param depth - maximum depth for recursion
   * @param regex - regex used to filter results.  Only results with file names that match the regex will be returned
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  public List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String relPathStr, boolean followLinks, int depth, String regex)
          throws ServiceException
  {
    List<FileInfo> listing = new ArrayList<>();
    // Make the call that does recursion
    listDirectoryRecurse(client, relPathStr, listing, followLinks, 0, Math.min(depth, MAX_RECURSION), regex);
    return listing;
  }

  /**
   * Get FileInfo for a path
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sysId - System
   * @param pathStr - path on system relative to system rootDir
   * @param followLinks - if true, symlinks will be followed.  This really means that we will go look at the
   *                    file pointed to by the link to get it's attributes.  The path will not be updated to
   *                    the path of the file pointed to by the link - it just affects the attributes.
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return FileInfo or null if not found
   * @throws NotFoundException - requested path not found
   */
  public FileInfo getFileInfo(@NotNull ResourceRequestUser rUser, @NotNull String sysId, @NotNull String pathStr,
                              boolean followLinks, String impersonationId, String sharedCtxGrantor)
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
      // Get the connection and increment the reservation count
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      return client.getFileInfo(pathStr, followLinks);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
  }

  /**
   * Upload a file
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - System
   * @param pathStr - path on system relative to system rootDir
   * @param inStrm  data stream to be used when creating file
   * @throws ForbiddenException - user not authorized
   */
  public void upload(@NotNull ResourceRequestUser rUser, @NotNull String systemId, @NotNull String pathStr,
                     @NotNull InputStream inStrm)
          throws WebApplicationException
  {
    String opName = "upload";
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();
    // Fetch system with credentials including auth checks for system and path
    TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                           opName, systemId, relPathStr, Permission.MODIFY,
                                                           impersonationIdNull, sharedCtxGrantorNull);
    // Reserve a client connection, use it to perform the operation and then release it
    IRemoteDataClient client = null;
    try
    {
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      upload(client, relPathStr, inStrm);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, systemId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
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
            throws ServiceException, BadRequestException
    {

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
        var fInfo = client.getFileInfo(relPathStr, true);
        if (fInfo != null && fInfo.isDir())
        {
          String msg = LibUtils.getMsg("FILES_ERR_UPLOAD_DIR", client.getOboTenant(), client.getOboUser(),
                                       client.getSystemId(), relPathStr);
          throw new BadRequestException(msg);
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
      // Get the connection and increment the reservation count
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys, null, sharedCtxGrantor);
      mkdir(client, relPathStr);
    }
    catch (IOException | ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, sysId, pathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
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
      // this mkdir can fail in the case that we are making the same directory from multiple threads.  One thread
      // will succeed, but the others will fail.  I tried just checking to see if the directory exists in the case
      // that the mkdir fails, but that doesnt work if you ahve to make multiple directories (the rough equivalent
      // of mkdir -p).  So for now, I think the best I can do is synchronize the mkdirs.  This could slow things down
      // slightly, but it should be a relatively fast operations, and it's probably better to succeed eventually
      // than to fail.
      synchronized (SSHDataClient.class) {
        client.mkdir(relPathStr);
      }
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
   * @param systemId - System
   * @param srcPathStr - source path on system relative to system rootDir
   * @param dstPathStr - destination path on system relative to system rootDir
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public void moveOrCopy(@NotNull ResourceRequestUser rUser, @NotNull MoveCopyOperation op, @NotNull String systemId,
                           String srcPathStr, String dstPathStr)
            throws WebApplicationException
    {
      String opName = op.name().toLowerCase();
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      // Get normalized paths relative to system rootDir and protect against ../..
      String srcRelPathStr = PathUtils.getRelativePath(srcPathStr).toString();
      String dstRelPathStr = PathUtils.getRelativePath(dstPathStr).toString();

      // Fetch system with credentials including auth checks for system and source path
      TapisSystem sys;
      if (op.equals(MoveCopyOperation.COPY))
      {
        sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                   opName, systemId, srcRelPathStr, Permission.READ,
                                                   impersonationIdNull, sharedCtxGrantorNull);
      }
      else
      {
        sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                   opName, systemId, srcRelPathStr, Permission.MODIFY,
                                                   impersonationIdNull, sharedCtxGrantorNull);
      }

      // To simplify auth check fetch the system again with check for destination path.
      // Since we just did the fetch everything should be cached, so we do not expect much of a performance hit.
      // Also, this is the only place where we need to do the two checks so probably not worth refactoring the auth
      // checks. Might consider refactoring in the long term. Plus, if we ever support share with MODIFY it will change.
      sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                 opName, systemId, dstRelPathStr, Permission.MODIFY,
                                                 impersonationIdNull, sharedCtxGrantorNull);

      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
        moveOrCopy(client, op, srcRelPathStr, dstRelPathStr);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, systemId, srcRelPathStr, ex.getMessage());
        log.error(msg, ex);
        throw new WebApplicationException(msg, ex);
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
    try
    {
      // Get file info here, so we can do some basic checks independent of the system type
      FileInfo srcFileInfo = client.getFileInfo(srcRelPathStr, true);
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
      if (op.equals(MoveCopyOperation.MOVE)) {
        client.move(srcRelPathStr, dstRelPathStr);
        // Update permissions in the SK
        permsService.replacePathPrefix(oboTenant, oboUser, sysId, srcRelPathStr, dstRelPathStr);
      } else if (op.equals(MoveCopyOperation.SERVICE_MOVE_DIRECTORY_CONTENTS)
              || op.equals(MoveCopyOperation.SERVICE_MOVE_FILE_OR_DIRECTORY)) {
        NativeLinuxOpResult linuxOpResult = ((ISSHDataClient)client).dtnMove(srcRelPathStr, dstRelPathStr, op);
        if(linuxOpResult.getExitCode() != 0) {
          throw new IOException(linuxOpResult.toString());
        }
      } else {
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
   * @param systemId - System
   * @param pathStr - path on system relative to system rootDir
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    public void delete(@NotNull ResourceRequestUser rUser, @NotNull String systemId, @NotNull String pathStr)
            throws WebApplicationException
    {
      String opName = "delete";
      String oboTenant = rUser.getOboTenantId();
      String oboUser = rUser.getOboUserId();
      // Get normalized path relative to system rootDir and protect against ../..
      String relativePathStr = PathUtils.getRelativePath(pathStr).toString();

      // Fetch system with credentials including auth checks for system and path
      TapisSystem sys = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache, systemsCacheNoAuth, permsService,
                                                             opName, systemId, relativePathStr, Permission.MODIFY,
                                                             impersonationIdNull, sharedCtxGrantorNull);

      // Reserve a client connection, use it to perform the operation and then release it
      IRemoteDataClient client = null;
      try
      {
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
        // Delete files and permissions
        delete(client, relativePathStr);
        // Remove shares with recurse=true
        shareService.removeAllSharesForPathWithoutAuth(oboTenant, systemId, relativePathStr, true);
      }
      catch (IOException | ServiceException ex)
      {
        String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "delete", systemId, relativePathStr, ex.getMessage());
        log.error(msg, ex);
        throw new WebApplicationException(msg, ex);
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
        getZip(rUser, output, sys, pathStr);
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
      stream = getByteRange(rUser, sys, pathStr, range.getMin(), range.getMax());
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

    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getPaginatedBytes(rUser, sys, relPathStr, startPage);
        stream.transferTo(output);
      }
      catch (NotFoundException ex)
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

    StreamingOutput outStream = output -> {
      InputStream stream = null;
      try
      {
        stream = getAllBytes(rUser, sys, pathStr);
        stream.transferTo(output);
      }
      catch (NotFoundException ex)
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
      return client.getStream(relPathStr);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getAllBytes", sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Generate a stream for a range of bytes.
   *
   * @param pathStr - path on system relative to system rootDir
   * @throws ServiceException general service error
   */
  InputStream getByteRange(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String pathStr,
                           long startByte, long count)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    IRemoteDataClient client = null;
    try
    {
      // Get a remoteDataClient to stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      return client.getBytesByRange(relPathStr, startByte, count);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getByteRange", sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Stream content from object at path with support for pagination
   *
   * @param relPathStr - path on system relative to system rootDir
   * @return InputStream
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
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
      return client.getBytesByRange(relPathStr, startByte, startByte + 1023);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getPaginatedBytes", sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Generate a streaming zip archive of a target path.
   *
   * @param outputStream Stream receiving zip contents
   * @param pathStr - path on system relative to system rootDir
   * @throws ServiceException general service error
   */
  void getZip(@NotNull ResourceRequestUser rUser, @NotNull OutputStream outputStream, @NotNull TapisSystem sys,
              @NotNull String pathStr)
          throws ServiceException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String sysId = sys.getId();
    // Get normalized path relative to system rootDir and protect against ../..
    String relPathStr = PathUtils.getRelativePath(pathStr).toString();

    String cleanedRelativePathString = FilenameUtils.normalize(pathStr);
    cleanedRelativePathString = StringUtils.prependIfMissing(cleanedRelativePathString, "/");

    IRemoteDataClient client = null;
    // Create an output stream and start adding to it
    try (ZipOutputStream zipStream = new ZipOutputStream(outputStream))
    {
      // Get a remoteDataClient to do the listing and stream contents
      client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sys);
      // Step through a recursive listing up to some max depth
      List<FileInfo> listing = lsRecursive(client, relPathStr, true, MAX_RECURSION, IRemoteDataClient.NO_PATTERN);
      for (FileInfo fileInfo : listing)
      {
        // Build the path we will use for the zip entry
        // To relativize we need a leading slash
        Path fileInfoPath = Paths.get("/", fileInfo.getPath());
        Path zipRootPath = Paths.get("/", FilenameUtils.getPath(cleanedRelativePathString));
        Path currentPath = zipRootPath.relativize(fileInfoPath);

        // For final entry we do not want the leading slash
        String entryPath = StringUtils.removeStart(currentPath.toString(), "/");

        // the file info that we got from the directory listing will not follow links, so we must
        // get file info with followLinks=true to get the info for the link.
        if(fileInfo.isSymLink()) {
          FileInfo tmpInfo = getFileInfo(rUser, sysId, fileInfo.getPath(), true, null, null);
          if(tmpInfo == null) {
            log.warn("Could not get file info for path:" + fileInfo.getPath());
            continue;
          }
          fileInfo = tmpInfo;
        }

        // Always add an entry for a dir to be sure empty directories are included
        if (fileInfo.isDir())
        {
          addDirectoryToZip(zipStream, entryPath);
        } else if(fileInfo.isFile()) {
          try (InputStream inputStream = getAllBytes(rUser, sys, fileInfo.getPath()))
          {
            addFileToZip(zipStream, entryPath, inputStream);
          }
        } else {
          log.warn("Ignoring file type: " + fileInfo.getType() + " path: " + fileInfo.getPath());
        }
      }
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, "getZip", sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
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
   * @param pattern - Wildcard (glob) pattern or regex used to filter results.  Regex must be prefixed by "regex:".
   *                Only results with file names that match the regex will be returned.
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   */
  private void listDirectoryRecurse(@NotNull IRemoteDataClient client, String basePath, List<FileInfo> listing,
                                    boolean followLinks, int depth, int maxDepth, String pattern)
          throws ServiceException
  {
    List<FileInfo> currentListing = ls(client, basePath, MAX_LISTING_SIZE, 0, pattern);
    listing.addAll(currentListing);
    // If client is S3 we are done.
    if (SystemTypeEnum.S3.equals(client.getSystemType())) return;
    for (FileInfo fileInfo: currentListing)
    {
      if(followLinks && fileInfo.isSymLink()) {
        FileInfo tmpInfo = null;
        try {
          tmpInfo = client.getFileInfo(fileInfo.getPath(), true);
        } catch (IOException e) {
          log.error("Could not get file info for path:" + fileInfo.getPath(), e);
          continue;
        }
        if(tmpInfo == null) {
          log.warn("Could not get file info for path:" + fileInfo.getPath());
          continue;
        }
        fileInfo = tmpInfo;
      }
      if (fileInfo.isDir() && depth < maxDepth)
      {
        listDirectoryRecurse(client, fileInfo.getPath(), listing, followLinks, depth + 1, maxDepth, pattern);
      }
    }
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
