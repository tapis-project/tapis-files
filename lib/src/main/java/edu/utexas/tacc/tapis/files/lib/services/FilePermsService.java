package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/*
 * Service level methods for Tapis File permissions.
 * NOTE: Paths stored in SK for permissions and shares always relative to rootDir and always start with /
 */
@Service
public class FilePermsService
{
  private static final IRuntimeConfig settings = RuntimeSettings.get();
  private static final Logger log = LoggerFactory.getLogger(FilePermsService.class);

  // PERMSPEC is "files:tenant:READ:systemId:path
  private static final String PERMSPEC = "files:%s:%s:%s:%s";

  @Inject
  private ServiceClients serviceClients;
  @Inject
  private FilePermsCache permsCache;

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_FILES;
  private static String siteAdminTenantId;

  public static void setSiteAdminTenantId(String s) { siteAdminTenantId = s; }

  public void grantPermission(String tenantId, String username, String systemId, String path, Permission perm)
          throws ServiceException
  {
    // This avoids ambiguous path issues with the SK. basically ensures that
    // even if the path is dir/file1.txt the entry will be /dir/file1.txt
    // Also removes any trailing slashes if present, needed for SK permissions checks
    String pathStr = PathUtils.getSKRelativePath(path).toString();
    String permSpec = String.format(PERMSPEC, tenantId, perm, systemId, pathStr);
    try
    {
      getSKClient().grantUserPermission(tenantId, username, permSpec);
    }
    catch (TapisClientException ex)
    {
      String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "grant", systemId, pathStr, ex.getMessage());
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Update SK permissions after a file or directory move/copy
   * @param tenantId - obo tenant id
   * @param username - oboUser user id
   * @param systemId - system id
   * @param oldPath - old path
   * @param newPath - new path
   * @throws ServiceException when SK client throws a TapisClientException
   */
  public void replacePathPrefix(String tenantId, String username, String systemId, String oldPath, String newPath) throws ServiceException {
    try {
      // TODO: use getSKRelativePath()?
      oldPath = StringUtils.prependIfMissing(oldPath, "/");
      newPath = StringUtils.prependIfMissing(newPath, "/");
      int modified = getSKClient().replacePathPrefix(tenantId, "files", null, systemId, systemId, oldPath, newPath);
      log.debug(String.valueOf(modified));
    } catch (TapisClientException ex) {
      String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "grant", systemId, oldPath, ex.getMessage());
      throw new ServiceException(msg, ex);
    }
  }

  public boolean isPermitted(@NotNull String tenantId, @NotNull String username, @NotNull String systemId,
                             @NotNull String path, @NotNull Permission perm)
          throws ServiceException
  {
    String pathStr = PathUtils.getSKRelativePath(path).toString();
    return permsCache.checkPerm(tenantId, username, systemId, pathStr, perm);
  }

  public Permission getPermission(@NotNull String tenantId, @NotNull String username, @NotNull String systemId,
                                  @NotNull String path)
          throws ServiceException
  {
    String pathStr = PathUtils.getSKRelativePath(path).toString();
    return permsCache.fetchPerm(tenantId, username, systemId, pathStr);
  }

  public void revokePermission(String tenantId, String username, String systemId, String path) throws ServiceException
  {
    String pathStr = PathUtils.getSKRelativePath(path).toString();
    String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, pathStr);
    try
    {
      getSKClient().revokeUserPermission(tenantId, username, permSpec);
      permSpec = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, pathStr);
      getSKClient().revokeUserPermission(tenantId, username, permSpec);
    }
    catch (TapisClientException ex)
    {
      String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "revoke", systemId, pathStr, ex.getMessage());
      throw new ServiceException(msg, ex);
    }
  }

  public void removePathPermissionFromAllRoles(String tenantId, String username,  String systemId, String path)
          throws ServiceException
  {
    // TODO use getSKRelativePath()?
    // String pathStr = PathUtils.getSKRelativePath(path).toString();
    path = StringUtils.prependIfMissing(path, "/");
    try
    {
      // TODO If we use a normalized relative path (with or without prepended /) there will never be a trailing /
      //      Why is this check here? for S3?
      if (path.endsWith("/")) {
        String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, path);
        getSKClient().removePathPermissionFromAllRoles(tenantId, permSpec);
        String permSpec2 = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, path);
        getSKClient().removePathPermissionFromAllRoles(tenantId, permSpec2);
      } else {
        path = StringUtils.removeEnd(path, "/");
        String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, path);
        getSKClient().removePermissionFromAllRoles(tenantId, permSpec);
        String permSpec2 = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, path);
        getSKClient().removePermissionFromAllRoles(tenantId, permSpec2);
      }
    } catch (TapisClientException ex) {
      String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "revoke", systemId, path, ex.getMessage());
      throw new ServiceException(msg, ex);
    }
  }

  /**
   * Get Security Kernel client
   * Need to use serviceClients.getClient() every time because it checks for expired service jwt token and
   *   refreshes it as needed.
   * Files service always calls SK as itself.
   * @return SK client
   * @throws TapisClientException - for Tapis related exceptions
   */
  private SKClient getSKClient() throws TapisClientException
  {
    try { return serviceClients.getClient(SERVICE_NAME, siteAdminTenantId, SKClient.class); }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, siteAdminTenantId, SERVICE_NAME);
      throw new TapisClientException(msg, e);
    }
  }
}
