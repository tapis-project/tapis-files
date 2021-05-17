package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService.NativeLinuxOperation;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Contract;

/*
 * Interface for FileUtils Service
 * Annotate as an hk2 Contract in case we have multiple implementations
 */
@Contract
public interface IFileUtilsService
{
  FileStatInfo getStatInfo(@NotNull IRemoteDataClient client, @NotNull String path, boolean followLinks) throws ServiceException;

  NativeLinuxOpResult linuxOp(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull NativeLinuxOperation op,
                              @NotNull String arg, boolean recursive) throws TapisException, ServiceException;
}
