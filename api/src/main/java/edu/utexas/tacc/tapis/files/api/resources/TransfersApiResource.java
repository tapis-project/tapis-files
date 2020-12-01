package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.validators.ValidUUID;
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
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.UUID;


@Path("/v3/files/transfers")
public class TransfersApiResource {

    private final String EXAMPLE_TASK_ID = "6491c2a5-acb2-40ef-b2c0-bc1fc4cd7e6c";
    private Logger log = LoggerFactory.getLogger(TransfersApiResource.class);

    @Inject
    TransfersService transfersService;

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
            @Valid @Parameter(required = true) TransferTaskRequest transferTask,
            @Context SecurityContext securityContext) {
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
//            TransferTask task = transfersService.createTransfer(
//                    user.getName(),
//                    user.getTenantId(),
//                    transferTask.getSourceSystemId(),
//                    transferTask.getSourcePath(),
//                    transferTask.getDestinationSystemId(),
//                    transferTask.getDestinationPath()
//            );
//            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse();
            return Response.ok("ok").build();
        } catch (Exception ex) {
            log.error("createTransferTask", ex);
            throw new WebApplicationException("server error");
        }
    }
}
