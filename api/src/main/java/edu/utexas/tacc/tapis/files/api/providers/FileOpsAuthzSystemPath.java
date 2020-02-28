package edu.utexas.tacc.tapis.files.api.providers;
import edu.utexas.tacc.tapis.files.api.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;

@FileOpsAuthorization
public class FileOpsAuthzSystemPath implements ContainerRequestFilter {

    private Logger log = LoggerFactory.getLogger(FileOpsAuthzSystemPath.class);
    private AuthenticatedUser user;
    // PERMSPEC is "files:tenant:r,rw,*:systemId:path
    private String PERMSPEC = "files:%s:%s:%s:%s";

    @Inject private SKClient skClient;

    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {

        FileOpsAuthorization requiredPerms = resourceInfo.getResourceMethod().getAnnotation(FileOpsAuthorization.class);

        user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
        String username = user.getName();
        String tenantId = user.getTenantId();
        MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
        log.info(params.toString());
        String path = params.getFirst("path");
        String systemId = params.getFirst("systemId");

        if (StringUtils.isEmpty(systemId)) {
            throw new BadRequestException("systemID must be specified");
        }

        if (StringUtils.isEmpty(path)) {
            path = "/";
        }

        String permSpec = String.format(PERMSPEC, tenantId, requiredPerms.permsRequired().getLabel(), systemId, path);
        try {
            boolean isPermitted = skClient.isPermitted(username, permSpec);
            if (!isPermitted) {
                throw new NotAuthorizedException("Not authorized to access this file/folder");
            }
        } catch (TapisClientException e) {
            log.error("FileOpsAuthzSystemPath", e);
            throw new WebApplicationException("Something went wrong...");
        }



    }
}
