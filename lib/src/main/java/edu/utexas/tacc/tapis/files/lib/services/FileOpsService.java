package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import org.apache.commons.io.FilenameUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


@Service
public class FileOpsService implements IFileOpsService {

    private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);
    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;
    private final FilePermsService permsService;

    // 0 = tenantBaseUri, 1=systemId, 2=path
    private String TAPIS_FILES_URI_FORMAT = "tapis://{0}/{1}/{2}";

    @Inject
    public FileOpsService(FilePermsService permsService) {
        this.permsService = permsService;
    }


    @Override
    public List<FileInfo> ls(IRemoteDataClient client, @NotNull String path) throws ServiceException, NotFoundException, NotAuthorizedException {
        return ls(client, path, MAX_LISTING_SIZE, 0);
    }

    @Override
    public List<FileInfo> ls(IRemoteDataClient client, @NotNull String path, long limit, long offset) throws ServiceException, NotFoundException {
        try {
            String cleanedPath = FilenameUtils.normalize(path);
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), cleanedPath, FileInfo.Permission.MODIFY);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
            List<FileInfo> listing = client.ls(path, limit, offset);
            return listing;
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "listing",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void mkdir(IRemoteDataClient client, String path) throws ServiceException, NotAuthorizedException {
        try {
            String cleanedPath = FilenameUtils.normalize(path);
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), cleanedPath, FileInfo.Permission.MODIFY);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
            client.mkdir(cleanedPath);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "mkdir",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void insert(IRemoteDataClient client, String path, @NotNull InputStream inputStream) throws ServiceException, NotFoundException, NotAuthorizedException {
        try {
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.MODIFY);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
            client.insert(path, inputStream);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "insert",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }

    @Override
    public void move(IRemoteDataClient client, String path, String newPath) throws ServiceException, NotFoundException, NotAuthorizedException {
        try {
            boolean srcPermitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.MODIFY);
            boolean destPermitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), newPath, FileInfo.Permission.MODIFY);
            if (!srcPermitted || !destPermitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
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
    public void copy(IRemoteDataClient client, String path, String newPath) throws ServiceException, NotFoundException, NotAuthorizedException {
        try {
            boolean srcPermitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.READ);
            boolean destPermitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), newPath, FileInfo.Permission.MODIFY);
            if (!srcPermitted || !destPermitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
            client.copy(path, newPath);
        } catch (IOException ex) {
            String msg = Utils.getMsg("FILES_OPSC_ERR", client.getOboTenant(), client.getOboUser(), "copy",
                                      client.getSystemId(), path, ex.getMessage());
            log.error(msg, ex);
            throw new ServiceException(msg, ex);
        }
    }


    @Override
    public void delete(IRemoteDataClient client, @NotNull String path) throws ServiceException, NotFoundException, NotAuthorizedException {
        try {
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.MODIFY);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
            client.delete(path);
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
    public InputStream getStream(IRemoteDataClient client, String path) throws ServiceException, NotFoundException, NotAuthorizedException{

        try {
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.READ);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
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
    public InputStream getBytes(IRemoteDataClient client, @NotNull String path, long startByte, long count) throws ServiceException, NotFoundException, NotAuthorizedException {
        try  {
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.READ);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
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
    public InputStream more(IRemoteDataClient client, @NotNull String path, long startPage) throws ServiceException, NotFoundException, NotAuthorizedException{
        long startByte = (startPage - 1) * 1024;
        try  {
            boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.READ);
            if (!permitted) {
                String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
                throw new NotAuthorizedException(msg);
            }
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
    public void getZip(IRemoteDataClient client, @NotNull OutputStream outputStream, @NotNull String path) throws ServiceException, NotAuthorizedException {

        boolean permitted = permsService.isPermitted(client.getOboTenant(), client.getOboUser(), client.getSystemId(), path, FileInfo.Permission.READ);
        if (!permitted) {
            String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", client.getOboTenant(), client.getOboUser(), client.getSystemId(), path);
            throw new NotAuthorizedException(msg);
        }

        //TODO: This should be made for recursive listings
        List<FileInfo> listing = this.ls(client, path);
        try (ZipOutputStream zipStream = new ZipOutputStream(outputStream)) {
            for (FileInfo fileInfo: listing) {
                try (InputStream inputStream = this.getStream(client, fileInfo.getPath()) ) {
                    ZipEntry entry = new ZipEntry(fileInfo.getPath());
                    zipStream.putNextEntry(entry);
                    inputStream.transferTo(zipStream);
                    zipStream.closeEntry();
                    log.debug("Added {} to zip", fileInfo);
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
