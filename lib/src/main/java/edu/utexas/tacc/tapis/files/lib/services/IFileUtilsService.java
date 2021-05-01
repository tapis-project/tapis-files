package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.NotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface IFileUtilsService
{
  FileStatInfo getStatInfo(@NotNull IRemoteDataClient client, @NotNull String path) throws ServiceException;

  void linuxChmod(@NotNull IRemoteDataClient client, String path) throws ServiceException;

  void linuxChown(@NotNull IRemoteDataClient client, String path) throws ServiceException;

  void linuxChgrp(@NotNull IRemoteDataClient client, String path) throws ServiceException;
}
