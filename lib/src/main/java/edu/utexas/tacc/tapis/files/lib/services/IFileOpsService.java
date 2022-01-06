package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Contract;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
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
  List<FileInfo> ls(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path, long limit, long offset)
          throws WebApplicationException;
  List<FileInfo> ls(@NotNull IRemoteDataClient client, @NotNull String path, long limit, long offset) throws ServiceException;

  List<FileInfo> lsRecursive(@NotNull ResourceRequestUser rUser, @NotNull TapisSystem sys, @NotNull String path, int maxDepth)
          throws WebApplicationException;
  List<FileInfo> lsRecursive(@NotNull IRemoteDataClient client, @NotNull String path, int maxDepth) throws ServiceException;

  void upload(@NotNull ResourceRequestUser rUser, TapisSystem sys, String path, InputStream in) throws WebApplicationException;
  void upload(@NotNull IRemoteDataClient client, String path, InputStream in) throws ServiceException;

  void mkdir(@NotNull ResourceRequestUser rUser, TapisSystem sys, String path) throws WebApplicationException;
  void mkdir(@NotNull IRemoteDataClient client, String path) throws ServiceException;

//  void move(@NotNull ResourceRequestUser rUser, TapisSystem sys, String srcPath, String dstPath) throws WebApplicationException;
  void move(@NotNull IRemoteDataClient client, String srcPath, String dstPath) throws ServiceException;

//  void copy(@NotNull ResourceRequestUser rUser, TapisSystem sys, String srcPath, String dstPath) throws WebApplicationException;
  void copy(@NotNull IRemoteDataClient client, String srcPath, String dstPath) throws ServiceException;

//  void delete(@NotNull ResourceRequestUser rUser, TapisSystem sys, String path) throws WebApplicationException;
  void delete(@NotNull IRemoteDataClient client, String path) throws ServiceException, NotFoundException;

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
}
