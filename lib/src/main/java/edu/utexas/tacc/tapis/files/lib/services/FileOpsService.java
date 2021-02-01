package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.DataClientUtils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.utils.Constants;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


public class FileOpsService implements IFileOpsService {

    private static final Logger log = LoggerFactory.getLogger(FileOpsService.class);
    private final IRemoteDataClient client;
    private static final int MAX_LISTING_SIZE = Constants.MAX_LISTING_SIZE;

    public FileOpsService(@NotNull IRemoteDataClient client) throws ServiceException {
        try {
            this.client = client;
            this.client.connect();
        } catch (IOException ex) {
            throw new ServiceException("Could not connect to system");
        }
    }

    public IRemoteDataClient getClient() {
        return client;
    }

    @Override
    public List<FileInfo> ls(@NotNull String path) throws ServiceException, NotFoundException {
        return this.ls(path, MAX_LISTING_SIZE, 0);
    }

    @Override
    public List<FileInfo> ls(@NotNull String path, long limit, long offset) throws ServiceException, NotFoundException {
        try {
            List<FileInfo> listing = client.ls(path, limit, offset);
            return listing;
        } catch (IOException ex) {
            String message = "Listing failed  : " + ex.getMessage();
            log.error("ERROR", ex);
            throw new ServiceException(message);
        }
    }

    @Override
    public void mkdir(String path) throws ServiceException {
        try {
            String cleanedPath = FilenameUtils.normalize(path);
            client.mkdir(cleanedPath);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("mkdir failed : " + ex.getMessage());
        }
    }

    @Override
    public void insert(String path, @NotNull InputStream inputStream) throws ServiceException {
        try {
            client.insert(path, inputStream);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("insert failed", ex);
        }
    }

    @Override
    public void move(String path, String newPath) throws ServiceException, NotFoundException {
        try {
            client.move(path, newPath);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("move/rename failed", ex);
        }
    }

    @Override
    public void delete(@NotNull String path) throws ServiceException, NotFoundException {
        try {
            client.delete(path);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("delete failed", ex);
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
    public InputStream getStream(String path) throws ServiceException, NotFoundException {
        // Try with resources to auto close the stream
        // Pushing the InputStream to a bufferedInputStream is memory efficient
        // way to auto close the initial input stream.
        try {
            InputStream fileStream = client.getStream(path);
            return fileStream;
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        }
    }

    @Override
    public InputStream getBytes(@NotNull String path, long startByte, long count) throws ServiceException, NotFoundException {
        try  {
            InputStream fileStream = client.getBytesByRange(path, startByte, count);
            return fileStream;
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        }
    }

    @Override
    public InputStream more(@NotNull String path, long startPage) throws ServiceException, NotFoundException {
        long startByte = (startPage - 1) * 1024;
        try  {
            InputStream fileStream = client.getBytesByRange(path, startByte, startByte + 1023);
            return fileStream;
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        }
    }

    /**
     * Genereate a streaming zip archive of a target path.
     * @param outputStream
     * @param path
     * @return
     * @throws IOException
     */
    public void getZip(@NotNull OutputStream outputStream, @NotNull String path) throws ServiceException {

        List<FileInfo> listing = this.ls(path);
        try (ZipOutputStream zipStream = new ZipOutputStream(outputStream)) {
            for (FileInfo fileInfo: listing) {
                try (InputStream inputStream = this.getStream(fileInfo.getPath()) ) {
                    ZipEntry entry = new ZipEntry(fileInfo.getPath());
                    zipStream.putNextEntry(entry);
                    inputStream.transferTo(zipStream);
                    zipStream.closeEntry();
                    log.info("Added {} to zip", fileInfo);
                }
            }
        } catch (IOException ex) {
            String msg = String.format("Could not zip path: %s", path);
            throw new ServiceException(msg, ex);
        }
    }
}
