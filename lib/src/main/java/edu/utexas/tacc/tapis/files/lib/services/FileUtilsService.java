package edu.utexas.tacc.tapis.files.lib.services;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

import edu.utexas.tacc.tapis.files.lib.models.AclEntry;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.ISSHDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;

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

  public enum NativeLinuxFaclOperation {ADD, REMOVE, REMOVE_ALL, REMOVE_DEFAULT}
  public enum NativeLinuxFaclRecursion {LOGICAL, PHYSICAL, NONE}

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
   * @param pathStr - target path for operation
   * @param followLinks - When path is a symbolic link whether to get information about the link (false)
   *                      or the link target (true)
   * @return FileStatInfo
   * @throws ServiceException - General problem
   */
  public FileStatInfo getStatInfo(@NotNull IRemoteDataClient client, @NotNull String pathStr, boolean followLinks)
          throws ServiceException
  {
    if (!(client instanceof ISSHDataClient)) {
      String msg = LibUtils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    boolean isOwner = false;
    if (client.getSystem().getOwner() != null) isOwner = client.getSystem().getOwner().equals(client.getOboTenant());

    // Get normalized path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(pathStr).toString();
    try
    {
      // If not system owner explicitly check permissions
      if (!isOwner) LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                            relativePathStr, Permission.READ);
      // Make the remoteDataClient call
      return sshClient.getStatInfo(relativePathStr, followLinks);
    }
    catch (IOException ex)
    {
      String msg = LibUtils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), "getStatInfo",
                                client.getSystemId(), relativePathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Run a native linux operation: chown, chmod, chgrp
   * @param client - remote data client
   * @param pathStr - target path for operation
   * @param op - operation to perform
   * @param arg - argument for operation
   * @param recursive - flag indicating if operation should be applied recursively for directories
   * @return - result of running the command
   * @throws ServiceException - General problem
   */
  public NativeLinuxOpResult linuxOp(@NotNull IRemoteDataClient client, @NotNull String pathStr, @NotNull NativeLinuxOperation op,
                                     @NotNull String arg, boolean recursive)
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
    boolean isOwner = false;
    if (client.getSystem().getOwner() != null) isOwner = client.getSystem().getOwner().equals(client.getOboTenant());

    // Get normalized path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(pathStr).toString();
    try
    {
      // If not skipping due to ownership or sharedAppCtx then check permissions
      if (!isOwner)
      {
        LibUtils.checkPermitted(permsService, client.getOboTenant(), client.getOboUser(), client.getSystemId(),
                                relativePathStr, Permission.MODIFY);
      }
// TODO remove?
//      else
//      {
//        // Log that we have skipped the perm check
//        String msg = LibUtils.getMsg("FILES_AUTH_SHAREDCTX2", client.getOboTenant(), client.getOboUser(), op.name(),
//                                     client.getSystemId(), path, sharedAppCtxGrantor);
//        log.warn(msg);
//      }

      // Make the remoteDataClient call
      switch (op) {
        case CHMOD -> nativeLinuxOpResult = sshClient.linuxChmod(relativePathStr, arg, recursive);
        case CHOWN -> nativeLinuxOpResult = sshClient.linuxChown(relativePathStr, arg, recursive);
        case CHGRP -> nativeLinuxOpResult = sshClient.linuxChgrp(relativePathStr, arg, recursive);
      }

    } catch (IOException ex) {
      String msg = LibUtils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), op.name(),
                                client.getSystemId(), relativePathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }
    return nativeLinuxOpResult;
  }

  public List<AclEntry> getfacl(@NotNull IRemoteDataClient client, @NotNull String pathStr)
          throws TapisException, ServiceException {
    List<AclEntry> aclEntries = Collections.emptyList();
    if (!(client instanceof ISSHDataClient)) {
      String msg = LibUtils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
              ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    boolean isOwner = false;
    if (client.getSystem().getOwner() != null) {
      isOwner = client.getSystem().getOwner().equals(client.getOboTenant());
    }

    // Get normalized path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(pathStr).toString();
    try {
      aclEntries = sshClient.runLinuxGetfacl(relativePathStr);
    } catch (IOException ex) {
      String msg = LibUtils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), "getfacl",
              client.getSystemId(), relativePathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }

    return aclEntries;
  }
  public NativeLinuxOpResult setfacl(@NotNull IRemoteDataClient client, @NotNull String pathStr,
                                     NativeLinuxFaclOperation operation,
                                     NativeLinuxFaclRecursion recursion, String aclEntries)
          throws TapisException, ServiceException {
    NativeLinuxOpResult nativeLinuxOpResult = NATIVE_LINUX_OP_RESULT_NOOP;
    if (!(client instanceof ISSHDataClient)) {
      String msg = LibUtils.getMsg("FILES_CLIENT_INVALID", client.getOboTenant(), client.getOboUser(), client.getSystemId(),
              ISSHDataClient.class.getSimpleName(), client.getClass().getSimpleName());
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    ISSHDataClient sshClient = (ISSHDataClient) client;
    boolean isOwner = false;
    if (client.getSystem().getOwner() != null) {
      isOwner = client.getSystem().getOwner().equals(client.getOboTenant());
    }

    // Get normalized path relative to system rootDir and protect against ../..
    String relativePathStr = PathUtils.getRelativePath(pathStr).toString();
    try {
      nativeLinuxOpResult = sshClient.runLinuxSetfacl(relativePathStr, operation, recursion, aclEntries);
    } catch (IOException ex) {
      String msg = LibUtils.getMsg("FILES_UTILS_CLIENT_ERR", client.getOboTenant(), client.getOboUser(), "getfacl",
              client.getSystemId(), relativePathStr, ex.getMessage());
      log.error(msg, ex);
      throw new ServiceException(msg, ex);
    }

    return nativeLinuxOpResult;
  }
}
