package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.NotFoundException;
import java.io.IOException;

public interface ISSHDataClient extends IRemoteDataClient
{
  // ------------------------------
  // Native Linux Utility Methods
  // ------------------------------
  FileStatInfo getStatInfo(@NotNull String remotePath) throws IOException, NotFoundException;
  void linuxChmod(@NotNull  String remotePath) throws IOException, NotFoundException;
  void linuxChown(@NotNull  String remotePath) throws IOException, NotFoundException;
  void linuxChgrp(@NotNull  String remotePath) throws IOException, NotFoundException;
}
