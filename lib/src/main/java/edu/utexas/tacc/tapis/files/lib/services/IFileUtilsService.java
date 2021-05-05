package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService.NativeLinuxOperation;

import org.jetbrains.annotations.NotNull;

public interface IFileUtilsService
{
  FileStatInfo getStatInfo(@NotNull IRemoteDataClient client, @NotNull String path, boolean followLinks) throws ServiceException;

  void linuxOp(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull NativeLinuxOperation op) throws ServiceException;
}
