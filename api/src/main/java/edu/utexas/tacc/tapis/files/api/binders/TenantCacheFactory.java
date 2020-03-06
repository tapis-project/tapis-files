package edu.utexas.tacc.tapis.files.api.binders;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.sharedapi.security.TenantCache;
import edu.utexas.tacc.tapis.tenants.client.TenantsClient;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
public class TenantCacheFactory implements Factory<TenantCache> {

    private static final Logger log = LoggerFactory.getLogger(TenantCacheFactory.class);

    private static IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Inject
    TenantsClient tenantsClient;

    @Override
    public TenantCache provide() {
        try {
            return new TenantCache(tenantsClient, "https://master.develop.tapis.io");
        } catch (Exception ex) {
            log.error("COULD NOT INIT TENANTS, DYING", ex);
            System.exit(1);
        }
        return null;
    }

    @Override
    public void dispose(TenantCache tenantCache) {

    }
}