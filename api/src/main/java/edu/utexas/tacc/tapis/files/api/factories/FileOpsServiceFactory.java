package edu.utexas.tacc.tapis.files.api.factories;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

public class FileOpsServiceFactory implements Factory<FileOpsService> {

    private final Logger log = LoggerFactory.getLogger(FileOpsServiceFactory.class);

    @Context
    UriInfo uriInfo;

    @Inject
    SystemsClient systemsClient;

    @Override
    public FileOpsService provide() {

        try {
            String systemId = uriInfo.getPathParameters().getFirst("systemId");
            return new FileOpsService(systemsClient, systemId);
        } catch (ServiceException e) {
            log.error("FileOpsServiceFactory:provide", e.getMessage());
            return null;
        }
    }

    @Override
    public void dispose(FileOpsService fileOpsService) {
        // noop
    }
}