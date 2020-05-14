package edu.utexas.tacc.tapis.files.api.binders;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.TenantsClient;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Inject;

public class TenantCacheFactory implements Factory<TenantManager> {

    private static final Logger log = LoggerFactory.getLogger(TenantCacheFactory.class);

    private final IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Override
    public TenantManager provide() {
        try {
            TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
            tenantManager.getTenants();
            return tenantManager;
        } catch (Exception ex) {
            log.error("COULD NOT INIT TENANTS, DYING", ex);
            System.exit(1);
        }
        return null;
    }

    @Override
    public void dispose(TenantManager tenantCache) {

    }
}