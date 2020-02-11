package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import org.glassfish.jersey.spi.Contract;

import java.io.InputStream;
import java.util.List;

@Contract
public interface IFileOpsService {

    List<FileInfo> ls(String path) throws ServiceException;
    void mkdir(String path) throws ServiceException;
    void move(String path, String newPath) throws ServiceException;
    void delete(String path) throws ServiceException;
    void insert(String path, InputStream in) throws ServiceException;
    InputStream getStream(String path) throws ServiceException;

}
