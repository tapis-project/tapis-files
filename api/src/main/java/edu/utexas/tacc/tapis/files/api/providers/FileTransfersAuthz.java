package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
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
import java.util.UUID;

@FileTransfersAuthorization
public class FileTransfersAuthz implements ContainerRequestFilter {

    private Logger log = LoggerFactory.getLogger(FileTransfersAuthz.class);
    private AuthenticatedUser user;

    @Inject private TransfersService transfersService;

    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {


        //TODO:

        user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
        String username = user.getName();
        String tenantId = user.getTenantId();
        MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
        String taskID = params.getFirst("transferTaskId");
        UUID taskUUID = UUID.fromString(taskID);

        try {
            boolean isPermitted = transfersService.isPermitted(username, tenantId, taskUUID);
            if (!isPermitted) {
                throw new NotAuthorizedException(Utils.getMsgAuth("FILES_TXFR_NOT_AUTH", user, taskUUID));
            }

        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, "authorization", ex.getMessage());
            log.error(msg, ex);
            throw new IOException(msg, ex);
        }





    }
}
