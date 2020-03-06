package edu.utexas.tacc.tapis.files.api.binders;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTCache;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;

public class ServiceJWTCacheFactory implements Factory<ServiceJWTCache> {

    IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Inject
    TokensClient tokensClient;

    @Override
    public ServiceJWTCache provide() {
        return new ServiceJWTCache(tokensClient, runtimeConfig.getServicePassword(), "files");
    }

    @Override
    public void dispose(ServiceJWTCache serviceJWTManager) {

    }
}
