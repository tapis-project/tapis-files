package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import javax.inject.Inject;


@Service
public class FilePermsService {

    private final SKClient skClient;
    private final TenantManager tenantManager;
    private final ServiceJWT serviceJWT;
    private final FilePermsCache permsCache;
    private static final IRuntimeConfig settings = RuntimeSettings.get();

    // PERMSPEC is "files:tenant:r,rw,*:systemId:path
    private static final String PERMSPEC = "files:%s:%s:%s:%s";

    @Inject
    public FilePermsService(TenantManager tenantManager, ServiceJWT serviceJWT, SKClient skClient, FilePermsCache permsCache) {
        this.tenantManager  = tenantManager;
        this.serviceJWT = serviceJWT;
        this.skClient = skClient;
        this.permsCache = permsCache;
    }

    public synchronized void grantPermission(String tenantId, String username, String systemId, String path, Permission perm) throws ServiceException {
        try {
            Tenant tenant = tenantManager.getTenant(tenantId);
            configClient(tenant, username);
            String permSpec = String.format(PERMSPEC, tenantId, perm, systemId, path);
            skClient.grantUserPermission(tenantId, username, permSpec);
        } catch (TapisException ex) {
            throw new ServiceException("Invalid tenant!", ex);
        } catch (TapisClientException ex) {
            String msg = Utils.getMsg("FILES_PERMC_ERR", tenantId, username, "grant", systemId, path, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    public synchronized boolean isPermitted(@NotNull String tenantId, @NotNull String username, @NotNull String systemId, @NotNull String path, @NotNull Permission perm) throws ServiceException {
        return permsCache.checkPerm(tenantId, username, systemId, path, perm);
    }

    public synchronized Permission getPermission(@NotNull String tenantId, @NotNull String username, @NotNull String systemId, @NotNull String path) throws ServiceException {
        return permsCache.fetchPerm(tenantId, username, systemId, path);
  }

    public synchronized void revokePermission(String tenantId, String username, String systemId, String path) throws ServiceException {
        try {
            Tenant tenant = tenantManager.getTenant(tenantId);
            configClient(tenant, username);
            String permSpec = String.format(PERMSPEC, tenantId, Permission.READ, systemId, path);
            skClient.revokeUserPermission(tenantId, username, permSpec);
            permSpec = String.format(PERMSPEC, tenantId, Permission.MODIFY, systemId, path);
            skClient.revokeUserPermission(tenantId, username, permSpec);
        } catch (TapisException ex) {
            throw new ServiceException("Invalid tenant!", ex);
        } catch (TapisClientException ex) {
            String msg = Utils.getMsg("FILES_PERMC_ERR", tenantId, username, "revoke", systemId, path, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    private synchronized void configClient(Tenant tenant, String username) {
        skClient.setUserAgent("filesServiceClient");
        skClient.setBasePath(tenant.getBaseUrl() + "/v3");
        skClient.addDefaultHeader("x-tapis-token", serviceJWT.getAccessJWT(settings.getSiteId()));
        skClient.addDefaultHeader("x-tapis-user", username);
        skClient.addDefaultHeader("x-tapis-tenant", tenant.getTenantId());
    }

}
