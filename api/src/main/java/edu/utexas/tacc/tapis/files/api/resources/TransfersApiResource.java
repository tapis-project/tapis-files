package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.validators.ValidUUID;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;


import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import java.util.List;
import java.util.UUID;


@Path("/v3/files/transfers")
public class TransfersApiResource {

    private final String EXAMPLE_TASK_ID = "6491c2a5-acb2-40ef-b2c0-bc1fc4cd7e6c";
    private Logger log = LoggerFactory.getLogger(TransfersApiResource.class);
    
    @Inject
    TransfersService transfersService;
    
    @Inject
    FilePermsService filePermsService; 

    private static class TransferTaskResponse extends TapisResponse<TransferTask>{}

    private void isPermitted(TransferTask task, AuthenticatedUser user) throws NotAuthorizedException {
        if (!task.getUsername().equals(user.getName())) throw new NotAuthorizedException("");
        if (!task.getTenantId().equals(user.getTenantId())) throw new NotAuthorizedException("");
    }

    @GET
    @Path("/{transferTaskId}/")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a transfer task", description = "", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TransferTaskResponse.class))
            )
    })
    public Response getTransferTask(
            @ValidUUID
            @Parameter(description = "Transfer task ID", required=true, example = EXAMPLE_TASK_ID)
            @PathParam("transferTaskId") String transferTaskId,
            @Context SecurityContext securityContext) {


        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();

        try {
            UUID transferTaskUUID = UUID.fromString(transferTaskId);
            TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
            isPermitted(task, user);
            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
            return Response.ok(resp).build();
        } catch (NotAuthorizedException e) {
        	log.error("Permission Denied", e);
			throw new WebApplicationException(Response
			          .status(Status.FORBIDDEN)
			          .type(MediaType.TEXT_PLAIN)
			          .entity("Permission denied")
			          .build());
        } catch (ServiceException e) {
            log.error("getTransferTaskStatus", e);
            throw new WebApplicationException("server error");
        }
    }

    @GET
    @Path("/{transferTaskId}/history/")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get history of a transfer task", description = "", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TransferTaskResponse.class))
            )
    })
    public Response getTransferTaskHistory(
            @ValidUUID
            @Parameter(description = "Transfer task ID", required=true, example = EXAMPLE_TASK_ID)
            @PathParam("transferTaskId") UUID transferTaskId,
            @Context SecurityContext securityContext) {


        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        // TODO BRANDI HERE
        return Response.ok().build();
    }


    @DELETE
    @Path("/{transferTaskId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Stop/Cancel a transfer task", description = "", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TapisResponse.class))
            )
    })
    public Response cancelTransferTask(
            @ValidUUID @Parameter(description = "Transfer task ID",required=true, example = EXAMPLE_TASK_ID) @PathParam("transferTaskId") String transferTaskId,
            @Context SecurityContext securityContext) {

        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();

        try {
            UUID transferTaskUUID = UUID.fromString(transferTaskId);
            TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
            isPermitted(task, user);
            transfersService.cancelTransfer(task);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse(null);
            resp.setMessage("Transfer deleted.");
            return Response.ok(resp).build();
        } catch (ServiceException e) {
            log.error("ERROR: cancelTransferTask", e);
            throw new WebApplicationException("server error");
        }
    }


    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Transfer data",
            description = "This creates a background task which will transfer files into the storage system",
            tags={ "transfers" }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TransferTaskResponse.class))
            )
    })
    public Response createTransferTask(
            @Valid @Parameter(required = true) TransferTaskRequest transferTaskRequest,
            @Context SecurityContext securityContext) {
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        
        // Input checking. 
        // Make sure the user has access READ access to the source URI and READWRITE access to the destination URI. 
        @NotEmpty List<TransferTaskRequestElement> uris = transferTaskRequest.getElements();
        try {
			TapisUrl destTapisUri = TapisUrl.makeTapisUrl(uris.get(0).getDestinationURI());
			checkTransferUriPermissions(user, destTapisUri, FilePermissionsEnum.READWRITE); 
			
			TapisUrl sourceTapisUri = TapisUrl.makeTapisUrl(uris.get(0).getSourceURI());
			checkTransferUriPermissions(user, sourceTapisUri, FilePermissionsEnum.READ); 
		} catch (TapisException e) {
			log.error("Permission Denied", e);
			throw new WebApplicationException(Response
			          .status(Status.FORBIDDEN)
			          .type(MediaType.TEXT_PLAIN)
			          .entity("Permission denied")
			          .build());
		} catch (ServiceException e) {
			log.error("ERROR: createTransferTask", e);
            throw new WebApplicationException("Error retrieving permissions.");
		}

        try {
            TransferTask task = transfersService.createTransfer(
                    user.getName(),
                    user.getTenantId(),
                    transferTaskRequest.getTag(),
                    transferTaskRequest.getElements()
            );

            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
            resp.setMessage("Transfer created.");
            return Response.ok(resp).build();
        } catch (Exception ex) {
            log.error("createTransferTask", ex);
            throw new WebApplicationException("server error");
        
        }
    }
    
    /**
     * Method that checks the user has provided permission on a given URI. 
     * @param user
     * @param tapisUrl
     * @param pem
     * @throws TapisException
     * @throws ServiceException
     */
    private void checkTransferUriPermissions(AuthenticatedUser user, TapisUrl tapisUrl, FilePermissionsEnum pem) 
    		throws TapisException, ServiceException {
			String system = tapisUrl.getHost(); 
			String uri = tapisUrl.toString(); 
			if (!filePermsService.isPermitted(user.getTenantId(), user.getName(), system, uri, pem))
				throw new TapisException("User does not have permission for transfer.");
			}
    }
