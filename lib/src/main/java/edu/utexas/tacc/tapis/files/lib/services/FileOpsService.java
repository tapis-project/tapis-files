package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
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
 *   Each public method uses provided IRemoteDataClient and other service library classes to perform all top level
 *   service operations.
 * Annotate as an hk2 Service so that default scope for Dependency Injection is singleton
 */
@Service
public class FileOpsService implements IFileOpsService
{
    private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);
    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;
    private final FilePermsService permsService;

    // 0=systemId, 1=path, 2=tenant
    private final String TAPIS_FILES_URL_FORMAT = "tapis://{0}/{1}?tenant={2}";
    private static final int MAX_RECURSION = 20;

    @Inject
    public FileOpsService(FilePermsService permsService) {
        this.permsService = permsService;
    }

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
      checkPermissions(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, Permission.READ);
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
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                 relativePath.toString(), path, Permission.READ);
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
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "listing",
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
    public void mkdir(IRemoteDataClient client, String path) throws ServiceException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), path, Permission.MODIFY);
        client.mkdir(relativePath.toString());
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "mkdir",
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
    public void insert(IRemoteDataClient client, String path, @NotNull InputStream inputStream)
            throws ServiceException, ForbiddenException
    {
        try {
            Path relativePath = PathUtils.getRelativePath(path);
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                 relativePath.toString(), path, Permission.MODIFY);
            client.insert(relativePath.toString(), inputStream);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "insert",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void move(IRemoteDataClient client, String path, String newPath)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        Path relativePathNew = PathUtils.getRelativePath(newPath);
        // Check the source and destination both have MODIFY perm
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), path, Permission.MODIFY);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePathNew.toString(), path, Permission.MODIFY);
        client.move(relativePath.toString(), relativePathNew.toString());
        // Update permissions in the SK
        permsService.replacePathPrefix(client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                       relativePath.toString(), relativePathNew.toString());
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "move",
                                  client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    @Override
    public void copy(IRemoteDataClient client, String path, String newPath)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        Path relativePathNew = PathUtils.getRelativePath(newPath);
        // Check the source has READ and destination has MODIFY perm
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), relativePath.toString(), Permission.READ);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePathNew.toString(), relativePath.toString(), Permission.MODIFY);
        client.copy(relativePath.toString(), relativePathNew.toString());
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "copy",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }


    @Override
    public void delete(IRemoteDataClient client, @NotNull String path)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), path, Permission.MODIFY);
        client.delete(relativePath.toString());
        permsService.removePathPermissionFromAllRoles(client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                                      relativePath.toString());
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "delete",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    /**
     * In order to have the method auto disconnect the client, we have to copy the
     * original InputStream from the client to another InputStream or else
     * the finally block immediately disconnects.
     *
     * @param path String
     * @return InputStream
     * @throws ServiceException
     */
    @Override
    public InputStream getStream(IRemoteDataClient client, String path)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try {
        Path relativePath = PathUtils.getRelativePath(path);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), path, Permission.READ);
        InputStream fileStream = client.getStream(relativePath.toString());
        return fileStream;
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "getContents",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    @Override
    public InputStream getBytes(IRemoteDataClient client, @NotNull String path, long startByte, long count)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      try  {
        Path relativePath = PathUtils.getRelativePath(path);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), path, Permission.READ);
        InputStream fileStream = client.getBytesByRange(path, startByte, count);
        return fileStream;
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "getBytes",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

    @Override
    public InputStream more(IRemoteDataClient client, @NotNull String path, long startPage)
            throws ServiceException, NotFoundException, ForbiddenException
    {
      long startByte = (startPage - 1) * 1024;
      try  {
        Path relativePath = PathUtils.getRelativePath(path);
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                             relativePath.toString(), path, Permission.READ);
        InputStream fileStream = client.getBytesByRange(path, startByte, startByte + 1023);
        return fileStream;
      } catch (IOException ex) {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "more",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
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
    @Override
    public void getZip(@NotNull IRemoteDataClient client, @NotNull OutputStream outputStream, @NotNull String path)
            throws ServiceException, ForbiddenException
    {
      Path relativePath = PathUtils.getRelativePath(path);
      Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                           relativePath.toString(), path, Permission.READ);
      String cleanedPath = FilenameUtils.normalize(path);
      cleanedPath = StringUtils.removeStart(cleanedPath, "/");
      if (StringUtils.isEmpty(cleanedPath)) cleanedPath = "/";
      // Step through a recursive listing up to some max depth
      List<FileInfo> listing = lsRecursive(client, path, MAX_RECURSION);
      try (ZipOutputStream zipStream = new ZipOutputStream(outputStream))
      {
        for (FileInfo fileInfo : listing)
        {
          // Always add an entry for a dir to be sure empty directories are included
          if (fileInfo.isDir())
          {
            ZipEntry entry = new ZipEntry(StringUtils.appendIfMissing(fileInfo.getPath(), "/"));
            zipStream.putNextEntry(entry);
            zipStream.closeEntry();
          }
          else
          {
            try (InputStream inputStream = this.getStream(client, fileInfo.getPath()))
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
      } catch (IOException ex)
      {
        String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "getZip",
                client.getSystemId(), path, ex.getMessage());
        log.error(msg, ex);
        throw new ServiceException(msg, ex);
      }
    }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  private void checkPermissions(String tenantId, String username, String systemId, String path, Permission permission)
          throws ServiceException, ForbiddenException
  {
    Path relativePath = PathUtils.getRelativePath(path);
    boolean permitted = permsService.isPermitted(tenantId, username, systemId, relativePath.toString(), permission);
    if (!permitted) {
      String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", tenantId, username, systemId, relativePath);
      throw new ForbiddenException(msg);
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
  private void listDirectoryRec(IRemoteDataClient client, String basePath, List<FileInfo> listing, int depth, int maxDepth)
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
