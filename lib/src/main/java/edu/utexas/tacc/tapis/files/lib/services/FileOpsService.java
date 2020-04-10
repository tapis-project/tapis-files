package edu.utexas.tacc.tapis.files.lib.services;

import java.io.*;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;



public class FileOpsService implements IFileOpsService {
    private Logger log = LoggerFactory.getLogger(FileOpsService.class);

    private IRemoteDataClient client;
    private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();

//    @Inject NotificationsServiceClient notificationsServiceClient;

    public FileOpsService(TSystem system) throws ServiceException {

        try {
            client = clientFactory.getRemoteDataClient(system);
            client.connect();
        } catch (IOException ex) {
            log.error("ERROR", ex);
            if (client != null) {
                client.disconnect();
            }
            throw new ServiceException("Could not connect to system");
        }
    }

    public IRemoteDataClient getClient() {
       return client;
   }

    @Override
    public void disconnect() {
        if (client != null) client.disconnect();
    }

    @Override
    public List<FileInfo> ls(String path) throws ServiceException {
        try {
            List<FileInfo> listing = client.ls(path);
            
            return listing;
        } catch (IOException ex) {
            String message = "Listing failed  : " + ex.getMessage();
            log.error("ERROR", ex);
            throw new ServiceException(message);
        } finally {
            client.disconnect();
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
        } finally {
            client.disconnect();
        }
    }

    @Override
    public void insert(String path, InputStream inputStream) throws ServiceException {
        try {
            client.insert(path, inputStream);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("insert failed");
        } finally {
            client.disconnect();
        }
    }

    @Override
    public void move(String path, String newPath) throws ServiceException {
        try {
            client.move(path, newPath);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("move/rename failed: " + ex.getMessage());
        } finally {
            client.disconnect();
        }
    }

    @Override
    public void delete(String path) throws ServiceException {
        try {
            client.delete(path);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("delete failed");
        } finally {
            client.disconnect();
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
    public InputStream getStream(String path) throws ServiceException {
        // Try with resources to auto close the stream
        try (InputStream fileStream = client.getStream(path)){
            InputStream out = IOUtils.toBufferedInputStream(fileStream);
            return out;
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        } finally {
            client.disconnect();
        }
    }

    @Override
    public InputStream getBytes(@NotNull String path, @NotNull long startByte, @NotNull long endByte) throws ServiceException  {
        try {
            return client.getBytesByRange(path, startByte, endByte);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        } finally {
            client.disconnect();
        }
    }

    @Override
    public InputStream more(@NotNull String path, @NotNull long startPage)  throws ServiceException {
        try {
            long startByte = (startPage -1) * 1024;
            return client.getBytesByRange(path, startByte, startByte + 1023);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        } finally {
           client.disconnect();
        }
    }
}
