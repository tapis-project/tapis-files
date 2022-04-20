package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
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


@Service
public class FilePermsService {

    private final FilePermsCache permsCache;
    private static final IRuntimeConfig settings = RuntimeSettings.get();
    private static final Logger log = LoggerFactory.getLogger(FilePermsService.class);

    // PERMSPEC is "files:tenant:READ:systemId:path
    private static final String PERMSPEC = "files:%s:%s:%s:%s";

    @Inject
    private ServiceClients serviceClients;

    @Inject
    public FilePermsService(FilePermsCache permsCache) {
        this.permsCache = permsCache;
    }

    public void grantPermission(String tenantId, String username, String systemId, String path, Permission perm) throws ServiceException {
        try {
            // This avoids ambiguous path issues with the SK. basically ensures that
            // even if the path is dir/file1.txt the entry will be /dir/file1.txt
            // Also removes any trailing slashes if present, needed for SK permissions checks
            path = StringUtils.removeEnd(path, "/");
            path = StringUtils.prependIfMissing(path, "/");
            SKClient skClient = getSKClient(tenantId, username);
            String permSpec = String.format(PERMSPEC, tenantId, perm, systemId, path);
            skClient.grantUserPermission(tenantId, username, permSpec);
        } catch (TapisClientException ex) {
            String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "grant", systemId, path, ex.getMessage());
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
            oldPath = StringUtils.prependIfMissing(oldPath, "/");
            newPath = StringUtils.prependIfMissing(newPath, "/");
            SKClient skClient = getSKClient(tenantId, username);
            int modified = skClient.replacePathPrefix(tenantId, "files", null, systemId, systemId, oldPath, newPath);
            log.debug(String.valueOf(modified));
        } catch (TapisClientException ex) {
            String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "grant", systemId, oldPath, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    public boolean isPermitted(@NotNull String tenantId, @NotNull String username, @NotNull String systemId, @NotNull String path, @NotNull Permission perm) throws ServiceException {
        path = StringUtils.removeEnd(path, "/");
        path = StringUtils.prependIfMissing(path, "/");
        return permsCache.checkPerm(tenantId, username, systemId, path, perm);
    }

    public Permission getPermission(@NotNull String tenantId, @NotNull String username, @NotNull String systemId, @NotNull String path) throws ServiceException {
        path = StringUtils.removeEnd(path, "/");
        path = StringUtils.prependIfMissing(path, "/");
        return permsCache.fetchPerm(tenantId, username, systemId, path);
    }

    public void revokePermission(String tenantId, String username, String systemId, String path) throws ServiceException {
        try {
            path = StringUtils.removeEnd(path, "/");
            path = StringUtils.prependIfMissing(path, "/");
            SKClient skClient = getSKClient(tenantId, username);
            String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, path);
            skClient.revokeUserPermission(tenantId, username, permSpec);
            permSpec = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, path);
            skClient.revokeUserPermission(tenantId, username, permSpec);
        } catch (TapisClientException ex) {
            String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "revoke", systemId, path, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    public void removePathPermissionFromAllRoles(String tenantId, String username,  String systemId, String path) throws ServiceException {
        try {
            path = StringUtils.prependIfMissing(path, "/");
            if (path.endsWith("/")) {
                SKClient skClient = getSKClient(tenantId, username);
                String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, path);
                skClient.removePathPermissionFromAllRoles(tenantId, permSpec);
                String permSpec2 = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, path);
                skClient.removePathPermissionFromAllRoles(tenantId, permSpec2);
            } else {
                path = StringUtils.removeEnd(path, "/");
                SKClient skClient = getSKClient(tenantId, username);
                String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, path);
                skClient.removePermissionFromAllRoles(tenantId, permSpec);
                String permSpec2 = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, path);
                skClient.removePermissionFromAllRoles(tenantId, permSpec2);
            }
        } catch (TapisClientException ex) {
            String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "revoke", systemId, path, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    /**
     * Get Security Kernel client
     *
     * @param tenantName
     * @param username
     * @return SK client
     * @throws TapisClientException - for Tapis related exceptions
     */
    private SKClient getSKClient(String tenantName, String username) throws TapisClientException {
        SKClient skClient;
        try {
            skClient = serviceClients.getClient(username, tenantName, SKClient.class);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_FILES, tenantName, username);
            throw new TapisClientException(msg, e);
        }
        return skClient;
    }
}
