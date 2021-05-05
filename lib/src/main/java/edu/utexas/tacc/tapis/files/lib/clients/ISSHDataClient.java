package edu.utexas.tacc.tapis.files.lib.clients;

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
  void linuxChmod(@NotNull  String remotePath) throws IOException, NotFoundException;
  void linuxChown(@NotNull  String remotePath) throws IOException, NotFoundException;
  void linuxChgrp(@NotNull  String remotePath) throws IOException, NotFoundException;
}
