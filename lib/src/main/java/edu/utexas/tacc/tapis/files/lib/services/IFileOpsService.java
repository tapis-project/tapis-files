package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import org.glassfish.jersey.spi.Contract;

import javax.validation.constraints.NotNull;
import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.util.List;

public interface IFileOpsService {

    List<FileInfo> ls(@NotNull String path) throws ServiceException;
    List<FileInfo> ls(@NotNull String path, long limit, long offset) throws ServiceException, NotFoundException;
    void mkdir(String path) throws ServiceException;
    void move(String path, String newPath) throws ServiceException;
    void delete(String path) throws ServiceException, NotFoundException;
    void insert(String path, InputStream in) throws ServiceException;
    InputStream getStream(String path) throws ServiceException, NotFoundException;
    InputStream getBytes(String path, long startByte, long endByte) throws ServiceException, NotFoundException;
    InputStream more(String path, long startByte) throws ServiceException;
    void disconnect();
}
