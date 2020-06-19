package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public abstract class BaseFilesResource {

    @Inject
    SystemsClient systemsClient;

    @Inject
    TenantManager tenantCache;

    @Inject
    ServiceJWT serviceJWTCache;

    private static final Logger log = LoggerFactory.getLogger(BaseFilesResource.class);

    /**
     * Configure the systems client with the correct baseURL and token for the request.
     * @param user
     * @throws ServiceException
     */
    void configureSystemsClient(AuthenticatedUser user) throws ServiceException {
        try {
            String tenantId = user.getTenantId();
            systemsClient.setBasePath(tenantCache.getTenant(tenantId).getBaseUrl());
            systemsClient.addDefaultHeader("x-tapis-token", serviceJWTCache.getAccessJWT());
            systemsClient.addDefaultHeader("x-tapis-user", user.getName());
            systemsClient.addDefaultHeader("x-tapis-tenant", user.getTenantId());
        } catch (TapisException ex) {
            String msg = "ERROR: configureSystems client failed for user: %s";
            log.error(String.format(msg, user.toString()), ex);
            throw new ServiceException("Something went wrong");
        }
    }

}
