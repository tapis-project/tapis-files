package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Contract;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/*
 * Interface for File Operations Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface IFileOpsService
{

  // ===================
  // GeneralOps
  // ===================
  TapisSystem getSystemIfEnabled(@NotNull ResourceRequestUser rUser, @NotNull String systemId) throws NotFoundException;

  // ===================
  // FileOps
  // ===================
  List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException;

  List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path, long limit, long offset)
          throws ServiceException, NotFoundException, ForbiddenException;

  List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String path, int maxDepth)
          throws ServiceException, NotFoundException, ForbiddenException;

  void mkdir(@NotNull IRemoteDataClient client, String path) throws ServiceException;

  void move(@NotNull IRemoteDataClient client, String srcPath, String dstPath) throws ServiceException;

  void copy(@NotNull IRemoteDataClient client, String srcPath, String dstPath) throws ServiceException;

  void delete(@NotNull IRemoteDataClient client, String path) throws ServiceException, NotFoundException;

  void upload(@NotNull IRemoteDataClient client, String path, InputStream in) throws ServiceException;

  // ===================
  // GetContents
  // ===================
  StreamingOutput getZipStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
          throws WebApplicationException;

  StreamingOutput getByteRangeStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                                     @NotNull HeaderByteRange range)
          throws WebApplicationException;

  StreamingOutput getPagedStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path,
                                 Long startPage)
          throws WebApplicationException;

  StreamingOutput getFullStream(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path)
          throws WebApplicationException;

//  // TODO/TBD: move these out of api resource and into private svc impl methods.
//  InputStream getStream(@NotNull IRemoteDataClient client, String path) throws ServiceException, NotFoundException;
//
//  InputStream getBytes(@NotNull IRemoteDataClient client, String path, long startByte, long endByte)
//          throws ServiceException, NotFoundException;
//
//  void getZip(@NotNull IRemoteDataClient client, OutputStream outputStream, String path) throws ServiceException;
//
//  InputStream more(@NotNull IRemoteDataClient client, String path, long startPAge)
//          throws ServiceException, NotFoundException;
}
