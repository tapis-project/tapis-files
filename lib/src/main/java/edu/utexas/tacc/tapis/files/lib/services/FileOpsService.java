package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;


@Service
public class FileOpsService implements IFileOpsService {
    private Logger log = LoggerFactory.getLogger(FileOpsService.class);

    private IRemoteDataClient client;
    private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();

    @Inject
    public FileOpsService(SystemsClient systemsClient, String systemId) throws ServiceException {

        try {
            // Fetch the system based on the systemId
            TSystem sys = systemsClient.getSystemByName(systemId,false,"");
            // Fetch the creds
            client = clientFactory.getRemoteDataClient(sys);
            client.connect();
        } catch (TapisClientException ex) {
           log.error("ERROR", ex);
           throw new ServiceException("");
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("Could not connect to system");
        } catch (Exception ex) {
            log.error("ERROR", ex);
            throw new ServiceException("");
        } 
    }

   public IRemoteDataClient getClient() {
       return client;
   }

    public FileOpsService(IRemoteDataClient remoteClient) throws ServiceException {
        client = remoteClient;
        try {
            client.connect();
        } catch (IOException ex) {
            client.disconnect();
            log.error("ERROR", ex);
            throw new ServiceException("Could not connect client");
        } finally {
            client.disconnect();
        }
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
        } 
    }

    @Override
    public void mkdir(String path) throws ServiceException {
        try {
            client.mkdir(path);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("mkdir failed : " + ex.getMessage());
        } 
    }

    @Override
    public void insert(String path, InputStream inputStream) throws ServiceException {
        try {
            client.insert(path, inputStream);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("insert failed");
        } 
    }

    @Override
    public void move(String path, String newPath) throws ServiceException {
        try {
            client.move(path, newPath);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("move/rename failed: " + ex.getMessage());
        } 
    }

    @Override
    public void delete(String path) throws ServiceException {
        try {
            client.delete(path);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("delete failed");
        } 
    }

    @Override
    public InputStream getStream(String path) throws ServiceException {
        try {
            return client.getStream(path);
        } catch (IOException ex) {
            log.error("ERROR", ex);
            throw new ServiceException("get contents failed");
        } 
    }


}
