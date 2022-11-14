package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.ISSHDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;

/*
 * Service level methods for FileUtils.
 *   Uses an SSHDataClient to perform top level service operations.
 *
 * NOTE: Paths stored in SK for permissions and shares always relative to rootDir and always start with /
 *
 * Annotate as an hk2 Service so that default scope for DI is singleton
 */
@Service
public class FileUtilsService
{

  private static final Logger log = LoggerFactory.getLogger(FileUtilsService.class);
  private final FilePermsService permsService;

  public enum NativeLinuxOperation {CHMOD, CHOWN, CHGRP}

  // Initial value for a NativeLinuxOpResult.
  public static final NativeLinuxOpResult NATIVE_LINUX_OP_RESULT_NOOP = new NativeLinuxOpResult("NO_OP", -1, "", "");

  @Inject
  public FileUtilsService(FilePermsService permsService) {
        this.permsService = permsService;
    }

  /**
   * Run the linux stat command on the path and return stat information
   *
   * @param client - remote data client
   * @param path - target path for operation
   * @param followLinks - When path is a symbolic link whether to get information about the link (false)
   *                      or the link target (true)
   * @return FileStatInfo
   * @throws ServiceException - General problem
   */
  public FileStatInfo getStatInfo(@NotNull IRemoteDataClient client, @NotNull String path, boolean followLinks)
          throws ServiceException
  {
    if (!(client instanceof ISSHDataClient)) {
      String msg = LibUtils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    try {
      // Check permissions
      LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                           path, Permission.READ);

      Path relativePath = PathUtils.getRelativePath(path);
      // Make the remoteDataClient call
      return sshClient.getStatInfo(relativePath.toString(), followLinks);

    } catch (IOException ex) {
      String msg = LibUtils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), "getStatInfo",
                                client.getSystemId(), path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Run a native linux operation: chown, chmod, chgrp
   * @param client - remote data client
   * @param path - target path for operation
   * @param op - operation to perform
   * @param arg - argument for operation
   * @param recursive - flag indicating if operation should be applied recursively for directories
   * @param sharedAppCtx - Indicates that request is part of a shared app context.
   * @return - result of running the command
   * @throws ServiceException - General problem
   */
  public NativeLinuxOpResult linuxOp(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull NativeLinuxOperation op,
                                     @NotNull String arg, boolean recursive, boolean sharedAppCtx)
          throws TapisException, ServiceException
  {
    NativeLinuxOpResult nativeLinuxOpResult = NATIVE_LINUX_OP_RESULT_NOOP;
    if (!(client instanceof ISSHDataClient))
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                   ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    try
    {
      // If not skipping due to sharedAppCtx then check permissions
      if (!sharedAppCtx)
      {
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                path, Permission.MODIFY);
      }
      else
      {
        // Log that we have skipped the perm check
        String msg = LibUtils.getMsg("FILES_AUTH_SHAREDAPPCTX2", client.getOboTenant(), client.getOboUser(), op.name(),
                                     client.getSystemId(), path);
        log.warn(msg);
      }

      Path relativePath = PathUtils.getRelativePath(path);
      // Make the remoteDataClient call
      switch (op) {
        case CHMOD -> nativeLinuxOpResult = sshClient.linuxChmod(relativePath.toString(), arg, recursive);
        case CHOWN -> nativeLinuxOpResult = sshClient.linuxChown(relativePath.toString(), arg, recursive);
        case CHGRP -> nativeLinuxOpResult = sshClient.linuxChgrp(relativePath.toString(), arg, recursive);
      }

    } catch (IOException ex) {
      String msg = LibUtils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), op.name(),
                                client.getSystemId(), path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    return nativeLinuxOpResult;
  }
}
