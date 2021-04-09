package edu.utexas.tacc.tapis.files.lib.factories;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import org.glassfish.hk2.api.Factory;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class ServiceContextFactory implements Factory<ServiceContext> {
    private static final Logger log = LoggerFactory.getLogger(ServiceContextFactory.class);

    private final IRuntimeConfig runtimeConfig = RuntimeSettings.get();

    @Override
    public ServiceContext provide() {
        ServiceContext context = ServiceContext.getInstance();
        try {
            context.initServiceJWT(runtimeConfig.getSiteId(), "files", runtimeConfig.getServicePassword());
            return context;
        } catch (TapisException | TapisClientException ex) {
            log.error("ERROR: could not get service JWT, exiting!", ex);
            System.exit(1);
            return null;
        }
    }

    @Override
    public void dispose(ServiceContext serviceContext) {

    }
}