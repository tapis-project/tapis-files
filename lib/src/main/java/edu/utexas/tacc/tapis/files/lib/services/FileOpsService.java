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

  /**
   * Check to see if a Tapis System exists and is enabled
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - System to check
   * @throws NotFoundException System not found or not enabled
   */
  public TapisSystem getSystemIfEnabled(@NotNull ResourceRequestUser rUser, @NotNull String systemId) throws NotFoundException
  {
    // Check for the system
    TapisSystem sys;
    try
    {
      sys = systemsCache.getSystem(rUser.getOboTenantId(), systemId, rUser.getOboUserId());
      if (sys == null) throw new NotFoundException(LibUtils.getMsgAuthR("FILES_SYS_NOTFOUND", rUser, systemId));
      if (sys.getEnabled() == null || !sys.getEnabled())
      {
        throw new NotFoundException(LibUtils.getMsgAuthR("FILES_SYS_NOTENABLED", rUser, systemId));
      }
    }
    catch (ServiceException ex)
    {
      throw new NotFoundException(LibUtils.getMsgAuthR("FILES_SYS_NOTFOUND", rUser, systemId));
    }
    return sys;
  }

  // ----------------------------------------------------------------------------------------------------
  // ------------- Support for FileOps
  // ----------------------------------------------------------------------------------------------------
  /**
   * List files at path
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path)
            throws ServiceException, NotFoundException, NotAuthorizedException
    {
        return ls(client, path, MAX_LISTING_SIZE, 0);
    }

  /**
   * Recursive list files at path
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @param maxDepth - maximum depth for recursion
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String path, int maxDepth)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      Path relativePath = PathUtils.getRelativePath(path);
      LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                           relativePath, Permission.READ);
      maxDepth = Math.min(maxDepth, MAX_RECURSION);
      List<FileInfo> listing = new ArrayList<>();
      listDirectoryRec(client, path, listing, 0, maxDepth);
      return listing;
    }

  /**
   * List files at path
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @return Collection of FileInfo objects
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws NotAuthorizedException - user not authorized
   */
    @Override
    public List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path, long limit, long offset)
            throws ServiceException, NotFoundException
    {
        try {
            Path relativePath = PathUtils.getRelativePath(path);
            LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                 relativePath, Permission.READ);
            List<FileInfo> listing = client.ls(relativePath.toString(), limit, offset);
            listing.forEach(f -> {
                String url = String.format("%s/%s", client.getSystemId(), f.getPath());
                url = StringUtils.replace(url, "//", "/");
                f.setUrl("tapis://" + url);
                // Ensure there is a leading slash
                f.setPath(StringUtils.prependIfMissing(f.getPath(), "/"));
            });
            return listing;
        } catch (IOException ex) {
            String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "listing",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

  /**
   * Create a directory at provided path.
   * Intermediate directories in the path will be created as necessary.
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void mkdir(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath, Permission.MODIFY);
        client.mkdir(relativePath.toString());
      } catch (IOException ex) {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "mkdir",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

  /**
   * Upload a file
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @param inputStream  data stream to be used when creating file
   * @throws ServiceException - general error
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void upload(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull InputStream inputStream)
            throws ServiceException, ForbiddenException
    {
        try {
            Path relativePath = PathUtils.getRelativePath(path);
            LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                 relativePath, Permission.MODIFY);
            client.upload(relativePath.toString(), inputStream);
        } catch (IOException ex) {
            String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "insert",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

  /**
   * Move a file or directory
   *
   * @param client remote data client to use
   * @param srcPath - source path on system relative to system rootDir
   * @param dstPath - destination path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void move(@NotNull IRemoteDataClient client, String srcPath, String dstPath)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path srcRelativePath = PathUtils.getRelativePath(srcPath);
        Path dstRelativePath = PathUtils.getRelativePath(dstPath);
        // Check the source and destination both have MODIFY perm
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             srcRelativePath, Permission.MODIFY);
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             dstRelativePath, Permission.MODIFY);
        // Get file info here so we can do some basic checks independent of the system type
        FileInfo srcFileInfo = client.getFileInfo(srcPath);
        // Make sure srcPath exists
        if (srcFileInfo == null)
        {
          String msg = LibUtils.getMsg("FILES_PATH_NOT_FOUND", client.getOboTenant(), client.getOboUser(),
                                    client.getSystemId(), srcPath);
          throw new NotFoundException(msg);
        }
        // If srcPath and dstPath are the same then throw an exception.
        if (srcRelativePath.equals(dstRelativePath))
        {
          String msg = LibUtils.getMsg("FILES_SRC_DST_SAME", client.getOboTenant(), client.getOboUser(),
                                    client.getSystemId(), srcPath);
          throw new NotFoundException(msg);
        }

        // Perform the operation
        client.move(srcRelativePath.toString(), dstRelativePath.toString());
        // Update permissions in the SK
        permsService.replacePathPrefix(client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                       srcRelativePath.toString(), dstRelativePath.toString());
      } catch (IOException ex) {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "move",
                                  client.getSystemId(), srcPath, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

  /**
   * Copy a file or directory
   * @param client remote data client to use
   * @param srcPath - source path on system relative to system rootDir
   * @param dstPath - destination path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void copy(@NotNull IRemoteDataClient client, String srcPath, String dstPath)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path srcRelativePath = PathUtils.getRelativePath(srcPath);
        Path dstRelativePath = PathUtils.getRelativePath(dstPath);
        // Check the source has READ and destination has MODIFY perm
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             srcRelativePath, Permission.READ);
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             dstRelativePath, Permission.MODIFY);

        // Get file info here so we can do some basic checks independent of the system type
        FileInfo srcFileInfo = client.getFileInfo(srcPath);
        // Make sure srcPath exists
        if (srcFileInfo == null)
        {
          String msg = LibUtils.getMsg("FILES_PATH_NOT_FOUND", client.getOboTenant(), client.getOboUser(),
                  client.getSystemId(), srcPath);
          throw new NotFoundException(msg);
        }
        // If srcPath and dstPath are the same then throw an exception.
        if (srcRelativePath.equals(dstRelativePath))
        {
          String msg = LibUtils.getMsg("FILES_SRC_DST_SAME", client.getOboTenant(), client.getOboUser(),
                  client.getSystemId(), srcPath);
          throw new NotFoundException(msg);
        }

        // Perform the operation
        client.copy(srcRelativePath.toString(), dstRelativePath.toString());
      } catch (IOException ex) {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "copy",
                client.getSystemId(), srcPath, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

  /**
   * Remove a file or directory
   * @param client remote data client to use
   * @param path - path on system relative to system rootDir
   * @throws ServiceException - general error
   * @throws NotFoundException - requested path not found
   * @throws ForbiddenException - user not authorized
   */
    @Override
    public void delete(@NotNull IRemoteDataClient client, @NotNull String path)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath, Permission.MODIFY);
        client.delete(relativePath.toString());
        permsService.removePathPermissionFromAllRoles(client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                                      relativePath.toString());
      } catch (IOException ex) {
        String msg = LibUtils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "delete",
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
      catch (NotFoundException ex)
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
    catch (NotFoundException ex)
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
      catch (NotFoundException ex)
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
      catch (NotFoundException ex)
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
          throws ServiceException, NotFoundException, ForbiddenException
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
          throws ServiceException, IOException, NotFoundException, ForbiddenException
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
          throws ServiceException, NotFoundException, ForbiddenException
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
          throws ServiceException, ForbiddenException
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
  private void listDirectoryRec(@NotNull IRemoteDataClient client, String basePath, List<FileInfo> listing, int depth, int maxDepth)
          throws ServiceException, NotFoundException
  {
    List<FileInfo> currentListing = ls(client, basePath);
    listing.addAll(currentListing);
    for (FileInfo fileInfo: currentListing) {
      if (fileInfo.isDir() && depth < maxDepth) {
        depth++;
        listDirectoryRec(client, fileInfo.getPath(), listing, depth, maxDepth);
      }
    }
  }
}
