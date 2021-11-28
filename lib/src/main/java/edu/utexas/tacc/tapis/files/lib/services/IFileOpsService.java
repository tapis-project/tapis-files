package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Contract;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.ZipOutputStream;

/*
 * Interface for File Operations Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface IFileOpsService {

    List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException;

    List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path, long limit, long offset) throws ServiceException, NotFoundException, ForbiddenException;

    List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String path, int maxDepth) throws ServiceException, NotFoundException, ForbiddenException;

    void mkdir(@NotNull IRemoteDataClient client, String path) throws ServiceException;

    void move(@NotNull IRemoteDataClient client, String path, String newPath) throws ServiceException;

    void copy(@NotNull IRemoteDataClient client, String path, String newPath) throws ServiceException;

    void delete(@NotNull IRemoteDataClient client, String path) throws ServiceException, NotFoundException;

    void upload(@NotNull IRemoteDataClient client, String path, InputStream in) throws ServiceException;

    InputStream getStream(@NotNull IRemoteDataClient client, String path) throws ServiceException, NotFoundException;

    InputStream getBytes(@NotNull IRemoteDataClient client, String path, long startByte, long endByte) throws ServiceException, NotFoundException;

    void getZip(@NotNull IRemoteDataClient client, OutputStream outputStream, String path) throws ServiceException;

    InputStream more(@NotNull IRemoteDataClient client, String path, long moreStartPAge) throws ServiceException, NotFoundException;
}
