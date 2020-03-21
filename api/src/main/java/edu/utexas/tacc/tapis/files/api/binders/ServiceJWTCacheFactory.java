package edu.utexas.tacc.tapis.files.api.binders;

import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthzSystemPath;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTParms;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class ServiceJWTCacheFactory implements Factory<ServiceJWT> {
    private Logger log = LoggerFactory.getLogger(ServiceJWTCacheFactory.class);

    IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Override
    public ServiceJWT provide() {
        try {
            ServiceJWTParms params = new ServiceJWTParms();
            params.setServiceName("files");
            return new ServiceJWT(params, runtimeConfig.getServicePassword());
        } catch (TapisException ex) {
            log.error("ERROR: could not get service JWT, exiting!", ex);
            System.exit(1);
            return null;
        }
    }

    @Override
    public void dispose(ServiceJWT serviceJWTManager) {

    }
}
