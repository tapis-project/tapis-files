package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.ISSHDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;

/*
 * Service level methods for FileUtils.
 *   Uses an SSHDataClient to perform top level service operations.
 * Annotate as an hk2 Service so that default scope for DI is singleton
 */
@Service
public class FileUtilsService implements IFileUtilsService {

  private static final Logger log = LoggerFactory.getLogger(FileUtilsService.class);
  private final FilePermsService permsService;

  public enum NativeLinuxOperation { CHMOD, CHOWN, CHGRP}

  // TODO
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
   * @throws NotFoundException - path not found
   * @throws NotAuthorizedException - user not authorized to operate on path
   */
  @Override
  public FileStatInfo getStatInfo(@NotNull IRemoteDataClient client, @NotNull String path, boolean followLinks)
          throws ServiceException, NotFoundException, NotAuthorizedException
  {
    if (!(client instanceof ISSHDataClient)) {
      String msg = Utils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    try {
      String cleanedPath = FilenameUtils.normalize(path);
      if (cleanedPath == null)
      {
        String msg = Utils.getMsg("FILES_CLIENT_SSH_NULL_PATH", sshClient.getOboTenant(), sshClient.getOboUser(),
                                  sshClient.getSystemId(), sshClient.getUsername(), sshClient.getHost(), path);
        throw new IllegalArgumentException(msg);
      }

      // Check permissions
      Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                           cleanedPath, path, Permission.READ);

      // Make the remoteDataClient call
      return sshClient.getStatInfo(cleanedPath, followLinks);

    } catch (IOException ex) {
      String msg = Utils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), "getStatInfo",
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
   * @return - result of running the command
   * @throws ServiceException - General problem
   * @throws NotAuthorizedException - user not authorized to operate on path
   */
  @Override
  public NativeLinuxOpResult linuxOp(@NotNull IRemoteDataClient client, @NotNull String path, @NotNull NativeLinuxOperation op,
                                     @NotNull String arg, boolean recursive)
          throws TapisException, ServiceException, NotAuthorizedException
  {
    NativeLinuxOpResult nativeLinuxOpResult = NATIVE_LINUX_OP_RESULT_NOOP;
    if (!(client instanceof ISSHDataClient)) {
      String msg = Utils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    try {
      String cleanedPath = FilenameUtils.normalize(path);
      // Check permissions
      Utils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                           cleanedPath, path, Permission.MODIFY);

      // Make the remoteDataClient call
      switch (op) {
        case CHMOD -> nativeLinuxOpResult = sshClient.linuxChmod(cleanedPath, arg, recursive);
        case CHOWN -> nativeLinuxOpResult = sshClient.linuxChown(cleanedPath, arg, recursive);
        case CHGRP -> nativeLinuxOpResult = sshClient.linuxChgrp(cleanedPath, arg, recursive);
      }

    } catch (IOException ex) {
      String msg = Utils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), op.name(),
                                client.getSystemId(), path, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    return nativeLinuxOpResult;
  }
}
