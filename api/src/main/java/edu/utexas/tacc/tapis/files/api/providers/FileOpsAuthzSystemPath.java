package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
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

    @Inject
    FilePermsService filePermsService;

    @Context
    private ResourceInfo resourceInfo;

    private IRuntimeConfig settings = RuntimeSettings.get();

    @Override
    public void filter(ContainerRequestContext requestContext) throws WebApplicationException {

        //This will be the annotation on the api method, which is one of the FilePermissionsEnum values
        FileOpsAuthorization requiredPerms = resourceInfo.getResourceMethod().getAnnotation(FileOpsAuthorization.class);

        final AuthenticatedUser user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
        String username = user.getOboUser();
        String tenantId = user.getOboTenantId();
        MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
        String path = params.getFirst("path");
        String systemId = params.getFirst("systemId");

        // Path might be optional, defaults to rootDir of system
        if (StringUtils.isEmpty(path)) {
            path = "/";
        }

        try {
            boolean isPermitted = filePermsService.isPermitted(tenantId, username, systemId, path, requiredPerms.permsRequired());
            if (!isPermitted) {
               throw new NotAuthorizedException("Authorization failed.");
            }
        } catch (ServiceException e) {
            // This should only happen when there is a network issue.
            String msg = Utils.getMsgAuth("FILES_OPS_ERROR", user, "authorization", systemId, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

}

