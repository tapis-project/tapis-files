package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

public abstract class BaseFilesResource {

    @Inject
    SystemsClient systemsClient;

    @Inject
    TenantManager tenantCache;

    @Inject
    ServiceJWT serviceJWTCache;

    @Inject
    RemoteDataClientFactory remoteDataClientFactory;

    private static final Logger log = LoggerFactory.getLogger(BaseFilesResource.class);
    private IRuntimeConfig settings = RuntimeSettings.get();

    /**
     * Configure the systems client with the correct baseURL and token for the request.
     * @param user
     * @throws ServiceException
     */
    public void configureSystemsClient(AuthenticatedUser user) throws ServiceException {
        try {
            String tenantId = user.getTenantId();
            systemsClient.setBasePath(tenantCache.getTenant(tenantId).getBaseUrl());
            systemsClient.addDefaultHeader("x-tapis-token", serviceJWTCache.getAccessJWT(settings.getSiteId()));
            systemsClient.addDefaultHeader("x-tapis-user", user.getName());
            systemsClient.addDefaultHeader("x-tapis-tenant", user.getTenantId());
        } catch (TapisException ex) {
            String msg = "ERROR: configureSystems client failed for user: %s";
            log.error(String.format(msg, user.toString()), ex);
            throw new ServiceException("Something went wrong");
        }
    }
    public IFileOpsService makeFileOpsService(TSystem system, String username) throws TapisClientException, IOException, ServiceException {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, username);
        FileOpsService fileOpsService = new FileOpsService(client);
        return fileOpsService;
    }
}
