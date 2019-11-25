package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.api.utils.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.security.Principal;


@Path("/transfers")
public class TransfersApiResource {

    private final String EXAMPLE_TASK_ID = "6491c2a5-acb2-40ef-b2c0-bc1fc4cd7e6c";
    private Logger log = LoggerFactory.getLogger(OperationsApiResource.class);

    @Inject
    FileTransfersDAO transfersDAO;

    private static class TransferTaskResponse extends TapisResponse<TransferTask>{}

    @GET
    @Path("/{transferTaskId}/")
    @Operation(summary = "Get the status of a transfer task", description = "", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TransferTaskResponse.class))
            )
    })
    public Response getTransferTaskStatus(
            @Parameter(description = "Transfer task ID",required=true, example = EXAMPLE_TASK_ID) @PathParam("transferTaskId") String transferTaskId,
            @Context SecurityContext securityContext) {

        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        log.info(user.toString());

        try {
            TransferTask task = transfersDAO.getTransferTask(transferTaskId);
            if (task == null) {
                return Response.status(404).build();
            }
            return Response.ok(task).build();
        } catch (DAOException e) {
            log.error("getTransferTaskStatus", e);
            return Response.status(500).build();
        }
    }


    @DELETE
    @Path("/{transferTaskId}")
    @Produces({ "application/json" })
    @Operation(summary = "Stop/Cancel a transfer task", description = "", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK"
            )
    })
    public Response cancelTransferTask(
            @Parameter(description = "Transfer task ID",required=true, example = EXAMPLE_TASK_ID) @PathParam("transferTaskId") String systemId,
            @Context SecurityContext securityContext) throws NotFoundException {
        return Response.ok().build();
    }

    @POST
    @Produces({ "application/json" })
    @Consumes({ "application/json" })
    @Operation(
            summary = "Transfer data",
            description = "This creates a background task which will transfer files into the storage system",
            tags={ "transfers" }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TransferTask.class))
            )
    })
    public Response createTransferTask(
            @Parameter(description = "Pet object that needs to be added to the store", required = true) TransferTaskRequest transferTask,
            @Context SecurityContext securityContext) throws NotFoundException {
        log.info(transferTask.toString());
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        log.info(user.toString());
        return Response.ok().build();
    }
}
