package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.NotFoundException;
import java.io.IOException;

public interface ISSHDataClient extends IRemoteDataClient
{
  // In addition to oboTenant, oboUser and systemId in IRemoteDataClient an SSHClient also has an
  //   associated user and host
  String getUsername();
  String getHost();

  // ------------------------------
  // Native Linux Utility Methods
  // ------------------------------
  FileStatInfo getStatInfo(@NotNull String remotePath, boolean followLinks) throws IOException, NotFoundException;
  void linuxChmod(@NotNull String remotePath, @NotNull String newPerms) throws ServiceException, IOException, NotFoundException;
  void linuxChown(@NotNull String remotePath, @NotNull String newOwner) throws IOException, NotFoundException;
  void linuxChgrp(@NotNull String remotePath, @NotNull String newGroup) throws IOException, NotFoundException;
}
