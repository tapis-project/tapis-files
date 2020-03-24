package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.*;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;

@FileOpsAuthorization
public class FileOpsAuthzSystemPath implements ContainerRequestFilter {

    private Logger log = LoggerFactory.getLogger(FileOpsAuthzSystemPath.class);
    private AuthenticatedUser user;
    // PERMSPEC is "files:tenant:r,rw,*:systemId:path
    private String PERMSPEC = "files:%s:%s:%s:%s";

    @Inject private SKClient skClient;

    @Inject private ServiceJWT serviceJWTCache;

    @Inject private TenantManager tenantCache;

    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) throws WebApplicationException {

        //This will be the annotation on the api method, which is one of the FilePermissionsEnum values
        FileOpsAuthorization requiredPerms = resourceInfo.getResourceMethod().getAnnotation(FileOpsAuthorization.class);

        user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
        String username = user.getName();
        String tenantId = user.getTenantId();
        MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
        String path = params.getFirst("path");
        String systemId = params.getFirst("systemId");

        // Path might be optional, defaults to rootDir of system
        if (StringUtils.isEmpty(path)) {
            path = "/";
        }
        String permSpec = String.format(PERMSPEC, tenantId, requiredPerms.permsRequired().getLabel(), systemId, path);
        
        try {
            Tenant tenant = tenantCache.getTenant(tenantId);
            skClient.setUserAgent("filesServiceClient");
            skClient.setBasePath(tenant.getBaseUrl() + "/v3");
            skClient.addDefaultHeader("x-tapis-token", serviceJWTCache.getAccessJWT());
            skClient.addDefaultHeader("x-tapis-user", username);
            skClient.addDefaultHeader("x-tapis-tenant", tenantId);
            boolean isPermitted = skClient.isPermitted(tenantId, username, permSpec);
            if (!isPermitted) {
               throw new NotAuthorizedException("Authorization failed.");
            }
        } catch (TapisException e) {
            // This should only happen when there is a network issue.
            log.error("ERROR: Files authorization failed", e);
            throw new WebApplicationException(e.getMessage());
        }
    }

}

