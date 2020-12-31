package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import java.security.Principal;


@Service
public class FilePermsService {

    private final SKClient skClient;
    private final TenantManager tenantManager;
    private final ServiceJWT serviceJWT;
    private static final IRuntimeConfig settings = RuntimeSettings.get();

    // PERMSPEC is "files:tenant:r,rw,*:systemId:path
    private static final String PERMSPEC = "files:%s:%s:%s:%s";

    @Inject
    public FilePermsService(TenantManager tenantManager, ServiceJWT serviceJWT, SKClient skClient) {
        this.tenantManager  = tenantManager;
        this.serviceJWT = serviceJWT;
        this.skClient = skClient;
    }

    public synchronized void grantPermission(String tenantId, Principal user, String systemId, String path, FilePermissionsEnum perm) throws ServiceException {
        try {
            Tenant tenant = tenantManager.getTenant(tenantId);
            configClient(tenant, user);
            String permSpec = formatPermSpec(tenantId, systemId, path, perm);
            skClient.grantUserPermission(tenantId, user.getName(), permSpec);
        } catch (TapisException ex) {
            throw new ServiceException("Invalid tenant!", ex);
        } catch (TapisClientException ex) {
            throw new ServiceException("Could not get permissions", ex);
        }
    }

    public synchronized boolean isPermitted(String tenantId, Principal user, String systemId, String path, FilePermissionsEnum perm) throws ServiceException {
        try {
            Tenant tenant = tenantManager.getTenant(tenantId);
            configClient(tenant, user);
            String permSpec = formatPermSpec(tenantId, systemId, path, perm);
            boolean isPermitted = skClient.isPermitted(tenantId, user.getName(), permSpec);
            return isPermitted;
        } catch (TapisException ex) {
            throw new ServiceException("Invalid tenant!", ex);
        } catch (TapisClientException ex) {
            throw new ServiceException("Could not get permissions", ex);
        }

    }

    public synchronized void revokePermission(String tenantId, Principal user, String systemId, String path, FilePermissionsEnum perm) throws ServiceException {
        try {
            Tenant tenant = tenantManager.getTenant(tenantId);
            configClient(tenant, user);
            String permSpec = formatPermSpec(tenantId, systemId, path, perm);
             skClient.revokeUserPermission(tenantId, user.getName(), permSpec);
        } catch (TapisException ex) {
            throw new ServiceException("Invalid tenant!", ex);
        } catch (TapisClientException ex) {
            throw new ServiceException("Could not get permissions", ex);
        }
    }

    private String formatPermSpec(String tenantId, String systemId, String path, FilePermissionsEnum permissionsEnum) {
        return String.format(PERMSPEC, tenantId, permissionsEnum, systemId, path);
    }

    private synchronized void configClient(Tenant tenant, Principal user) {
        skClient.setUserAgent("filesServiceClient");
        skClient.setBasePath(tenant.getBaseUrl() + "/v3");
        skClient.addDefaultHeader("x-tapis-token", serviceJWT.getAccessJWT(settings.getSiteId()));
        skClient.addDefaultHeader("x-tapis-user", user.getName());
        skClient.addDefaultHeader("x-tapis-tenant", tenant.getTenantId());
    }

}
