package edu.utexas.tacc.tapis.files.api.providers;
import com.jcraft.jsch.IO;
import edu.utexas.tacc.tapis.files.api.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
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

@FileTransfersAuthorization
public class FileTransfersAuthz implements ContainerRequestFilter {

    private Logger log = LoggerFactory.getLogger(FileOpsAuthzSystemPath.class);
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

        try {
            boolean isPermitted = transfersService.isPermitted(username, tenantId, taskID);
            if (!isPermitted) {
                throw new NotAuthorizedException("Not authorized to access this transfer");
            }

        } catch (ServiceException ex) {
            throw new IOException("Could not verify ownership of transfer task.");
        }





    }
}
