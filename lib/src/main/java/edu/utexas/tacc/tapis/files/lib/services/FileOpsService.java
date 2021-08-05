package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
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
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


@Service
public class FileOpsService implements IFileOpsService {

    private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);
    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;
    private final FilePermsService permsService;

    // 0 = tenantBaseUri, 1=systemId, 2=path
    private String TAPIS_FILES_URI_FORMAT = "tapis://{0}/{1}/{2}";
    private static final int MAX_RECURSION = 20;

    @Inject
    public FileOpsService(FilePermsService permsService) {
        this.permsService = permsService;
    }


    @Override
    public List<FileInfo> ls(IRemoteDataClient client, @NotNull String path) throws ServiceException, NotFoundException, NotAuthorizedException {
        return ls(client, path, MAX_LISTING_SIZE, 0);
    }

    private boolean checkPermissions(String tenantId, String username, String systemId, String path, FileInfo.Permission permission) throws ServiceException, ForbiddenException {
        String cleanedPath = FilenameUtils.normalize(path);
        boolean permitted = permsService.isPermitted(tenantId, username, systemId, cleanedPath, permission);
        if (!permitted) {
            String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", tenantId, username, systemId, cleanedPath);
            throw new ForbiddenException(msg);
        }
        return true;
    }

    private void listDirectoryRec(IRemoteDataClient client, String basePath, List<FileInfo> listing, int depth, int maxDepth) throws ServiceException, NotFoundException{
        List<FileInfo> currentListing = this.ls(client, basePath);
        listing.addAll(currentListing.stream().filter((f)->!f.isDir()).collect(Collectors.toList()));
        for (FileInfo fileInfo: currentListing) {
            if (fileInfo.isDir() && depth < maxDepth) {
                depth++;
                listDirectoryRec(client, fileInfo.getPath(), listing, depth, maxDepth);
            }
        }
    }

    @Override
    public List<FileInfo> lsRecursive(IRemoteDataClient client, @NotNull String path, int maxDepth) throws ServiceException, NotFoundException, ForbiddenException {
        maxDepth = Math.min(maxDepth, MAX_RECURSION);
        checkPermissions(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.READ);
        List<FileInfo> listing = new ArrayList<>();
        listDirectoryRec(client, path, listing, 0, maxDepth);
        return listing;
    }

    @Override
    public List<FileInfo> ls(IRemoteDataClient client, @NotNull String path, long limit, long offset) throws ServiceException, NotFoundException {
        try {
            String cleanedPath = FilenameUtils.normalize(path);
            if (StringUtils.isEmpty(cleanedPath)) cleanedPath = "/";
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), cleanedPath, path, Permission.READ);
            List<FileInfo> listing = client.ls(cleanedPath, limit, offset);
            listing.forEach(f -> {
                String uri = String.format("%s/%s/%s", client.getOboTenant(), client.getSystemId(), f.getPath());
                uri = StringUtils.replace(uri, "//", "/");
                f.setUri("tapis://" + uri);
            });
            return listing;
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "listing",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void mkdir(IRemoteDataClient client, String path) throws ServiceException, ForbiddenException {
        try {
            String cleanedPath = FilenameUtils.normalize(path);
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), cleanedPath, path, Permission.MODIFY);
            client.mkdir(cleanedPath);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "mkdir",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void insert(IRemoteDataClient client, String path, @NotNull InputStream inputStream) throws ServiceException, NotFoundException, ForbiddenException {
        try {
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.MODIFY);
            client.insert(path, inputStream);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "insert",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void move(IRemoteDataClient client, String path, String newPath) throws ServiceException, NotFoundException, ForbiddenException {
        try {
            // Check the source and destination both have MODIFY perm
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.MODIFY);
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), newPath, path, Permission.MODIFY);
            client.move(path, newPath);
            permsService.replacePathPrefix(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, newPath);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "move",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void copy(IRemoteDataClient client, String path, String newPath) throws ServiceException, NotFoundException, ForbiddenException {
        try {
            // Check the source has READ and destination has MODIFY perm
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.READ);
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), newPath, path, Permission.MODIFY);
            client.copy(path, newPath);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "copy",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }


    @Override
    public void delete(IRemoteDataClient client, @NotNull String path) throws ServiceException, NotFoundException, ForbiddenException {
        try {
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.MODIFY);
            client.delete(path);
            permsService.removePathPermissionFromAllRoles(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
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
    public InputStream getStream(IRemoteDataClient client, String path) throws ServiceException, NotFoundException, ForbiddenException{

        try {
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.READ);
            InputStream fileStream = client.getStream(path);
            return fileStream;
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "getContents",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public InputStream getBytes(IRemoteDataClient client, @NotNull String path, long startByte, long count) throws ServiceException, NotFoundException, ForbiddenException {
        try  {
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.READ);
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
    public InputStream more(IRemoteDataClient client, @NotNull String path, long startPage) throws ServiceException, NotFoundException, ForbiddenException {
        long startByte = (startPage - 1) * 1024;
        try  {
            Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.READ);
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
     * Genereate a streaming zip archive of a target path.
     * @param outputStream
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public void getZip(IRemoteDataClient client, @NotNull OutputStream outputStream, @NotNull String path) throws ServiceException, ForbiddenException {
        Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, path, Permission.READ);
        //TODO: This should be made for recursive listings]
        String cleanedPath = FilenameUtils.normalize(path);
        cleanedPath = StringUtils.removeStart(cleanedPath, "/");
        if (StringUtils.isEmpty(cleanedPath)) cleanedPath = "/";
        List<FileInfo> listing = this.lsRecursive(client, path, MAX_RECURSION);
        try (ZipOutputStream zipStream = new ZipOutputStream(outputStream)) {
            for (FileInfo fileInfo: listing) {
                try (InputStream inputStream = this.getStream(client, fileInfo.getPath()) ) {
                    String tmpPath = StringUtils.removeStart(fileInfo.getPath(), "/");
                    Path pth = Paths.get(cleanedPath).relativize(Paths.get(tmpPath));
                    ZipEntry entry = new ZipEntry(pth.toString());
                    zipStream.putNextEntry(entry);
                    inputStream.transferTo(zipStream);
                    zipStream.closeEntry();
                }
            }
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "getZip",
                                       client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }
}
