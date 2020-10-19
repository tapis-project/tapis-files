package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;

/**
 * SystemsClientFactory provides a way to easily inject a systems service client
 * and configure it for the proper tenant and credentials.
 */
@Service
public class SystemsClientFactory {

    private TenantManager tenantManager;
    private ServiceJWT serviceJWTCache;
    private IRuntimeConfig settings = RuntimeSettings.get();

    @Inject
    public SystemsClientFactory(TenantManager tenantManager, ServiceJWT serviceJWTCache) {
        this.tenantManager = tenantManager;
        this.serviceJWTCache = serviceJWTCache;
    }

    public SystemsClient getClient(String tenantId, String username) throws ServiceException {
        SystemsClient client = new SystemsClient();
        try {
            Tenant tenant = tenantManager.getTenant(tenantId);
            client.setBasePath(tenant.getBaseUrl());
            client.addDefaultHeader("x-tapis-token", serviceJWTCache.getAccessJWT(settings.getSiteId()));
            client.addDefaultHeader("x-tapis-user", username);
            client.addDefaultHeader("x-tapis-tenant", tenant.getTenantId());
            return client;
        } catch (TapisException ex) {
            throw new ServiceException("could not find tenant?");
        }
    }

}
