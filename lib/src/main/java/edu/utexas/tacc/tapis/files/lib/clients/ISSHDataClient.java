package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.AclEntry;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.List;

public interface ISSHDataClient extends IRemoteDataClient
{
  // ------------------------------
  // Native Linux Utility Methods
  // ------------------------------
  FileStatInfo getStatInfo(@NotNull String remotePath, boolean followLinks) throws IOException, NotFoundException;
  NativeLinuxOpResult linuxChmod(@NotNull String remotePath, @NotNull String newPerms, boolean recursive) throws TapisException, IOException, NotFoundException;
  NativeLinuxOpResult linuxChown(@NotNull String remotePath, @NotNull String newOwner, boolean recursive) throws TapisException, IOException, NotFoundException;
  NativeLinuxOpResult linuxChgrp(@NotNull String remotePath, @NotNull String newGroup, boolean recursive) throws TapisException, IOException, NotFoundException;
  List<AclEntry> runLinuxGetfacl(String path) throws IOException, TapisException;
  NativeLinuxOpResult runLinuxSetfacl(String path, FileUtilsService.NativeLinuxFaclOperation operation,
                                 FileUtilsService.NativeLinuxFaclRecursion recursion,
                                 String aclEntries) throws IOException, TapisException;
}
