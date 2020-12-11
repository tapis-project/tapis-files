package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.ServiceJWTParms;
import org.glassfish.hk2.api.Factory;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

@Service @Named
public class ServiceJWTCacheFactory implements Factory<ServiceJWT> {
    private static final Logger log = LoggerFactory.getLogger(ServiceJWTCacheFactory.class);

    private final IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Override
    public ServiceJWT provide() {
        try {
            List<String> targetSites = new ArrayList<>();
            targetSites.add(runtimeConfig.getSiteId());
            ServiceJWTParms params = new ServiceJWTParms();
            params.setTargetSites(targetSites);
            params.setServiceName("files");
            params.setTenant("admin");
            params.setTokensBaseUrl(runtimeConfig.getTokensServiceURL());
            return new ServiceJWT(params, runtimeConfig.getServicePassword());
        } catch (TapisException | TapisClientException ex) {
            log.error("ERROR: could not get service JWT, exiting!", ex);
            System.exit(1);
            return null;
        }
    }

    @Override
    public void dispose(ServiceJWT serviceJWTManager) {}
}
