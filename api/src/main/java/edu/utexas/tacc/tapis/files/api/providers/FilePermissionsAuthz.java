package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;


/**
 *  Annotation for The File Permissions endpoints. Currently, only the *owner* of the system
 *  is allowed to grant/revoke permissions. API users can check their own permissions
 *  via the GET endpoint, which is not protected by this annotation.
 */
@FilePermissionsAuthorization
public class FilePermissionsAuthz implements ContainerRequestFilter {

    private Logger log = LoggerFactory.getLogger(FilePermissionsAuthz.class);
    private AuthenticatedUser user;

    @Inject private SystemsCache systemsCache;

    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) throws NotAuthorizedException, IOException {

        user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
        String username = user.getName();
        String tenantId = user.getTenantId();
        MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
        String systemId = params.getFirst("systemId");

        try {
            TSystem system = systemsCache.getSystem(tenantId, systemId);
            if (!Objects.equals(system.getOwner(), username)) {
                throw new NotAuthorizedException("User is not authorized to grant permissions on this system");
            }
        } catch (ServiceException ex) {
            throw new IOException("Could not verify permissions.");
        }





    }
}
