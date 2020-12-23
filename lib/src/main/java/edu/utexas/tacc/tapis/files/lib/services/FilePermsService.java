package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import java.security.Principal;


@Service
public class FilePermsService {

    private final TenantManager tenantManager;
    private final ServiceJWT serviceJWT;
    private static final IRuntimeConfig settings = RuntimeSettings.get();

    @Inject
    public FilePermsService(TenantManager tenantManager, ServiceJWT serviceJWT) {
        this.tenantManager  = tenantManager;
        this.serviceJWT = serviceJWT;
    }

    public void grantPermission(Principal user, String systemId, String path, FilePermissionsEnum perm) throws ServiceException {

    }

    public void isPermitted(Principal user, String systemId, String path, FilePermissionsEnum perm) throws ServiceException {

    }

    public void revokePermission(Principal user, String systemId, String path, FilePermissionsEnum perm) throws ServiceException {

    }

    private SKClient configClient(Tenant tenant, Principal user) {
        SKClient skClient = new SKClient();
        skClient.setUserAgent("filesServiceClient");
        skClient.setBasePath(tenant.getBaseUrl() + "/v3");
        skClient.addDefaultHeader("x-tapis-token", serviceJWT.getAccessJWT(settings.getSiteId()));
        skClient.addDefaultHeader("x-tapis-user", user.getName());
        skClient.addDefaultHeader("x-tapis-tenant", tenant.getTenantId());
        return skClient;
    }


}
