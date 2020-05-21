package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTParms;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceJWTCacheFactory implements Factory<ServiceJWT> {
    private Logger log = LoggerFactory.getLogger(ServiceJWTCacheFactory.class);

    private final IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Override
    public ServiceJWT provide() {
        try {
            ServiceJWTParms params = new ServiceJWTParms();
            params.setServiceName("files");
            params.setTenant("master");
            params.setTokensBaseUrl(runtimeConfig.getTokensServiceURL());
            return new ServiceJWT(params, runtimeConfig.getServicePassword());
        } catch (TapisException ex) {
            log.error("ERROR: could not get service JWT, exiting!", ex);
            System.exit(1);
            return null;
        }
    }

    @Override
    public void dispose(ServiceJWT serviceJWTManager) {}
}
