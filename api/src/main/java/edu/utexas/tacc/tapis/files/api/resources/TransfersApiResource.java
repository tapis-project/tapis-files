package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
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
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.UUID;


@Path("/v3/files/transfers")
public class  TransfersApiResource {

    private static final String EXAMPLE_TASK_ID = "6491c2a5-acb2-40ef-b2c0-bc1fc4cd7e6c";
    private static final Logger log = LoggerFactory.getLogger(TransfersApiResource.class);

    @Inject
    TransfersService transfersService;

    @Inject
    FilePermsService permsService;

    private static class StringResponse extends TapisResponse<String>{}
    private static class TransferTaskResponse extends TapisResponse<TransferTask>{}
    private static class TransferTaskListResponse extends TapisResponse<List<TransferTask>>{}


    private void isPermitted(TransferTask task, AuthenticatedUser user) throws NotAuthorizedException {
        if (!task.getUsername().equals(user.getOboUser())) throw new NotAuthorizedException("");
        if (!task.getTenantId().equals(user.getOboTenantId())) throw new NotAuthorizedException("");
    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a list of recent transfer tasks starting with the most recent", description = "Get a list of recent transfer tasks starting with the most recent", tags={ "transfers" })
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "OK",
            content = @Content(schema = @Schema(implementation = TransferTaskListResponse.class))
        )
    })
    public Response getRecentTransferTasks(
        @Parameter(description = "pagination limit", example = "100") @DefaultValue("1000") @QueryParam("limit") @Max(1000) int limit,
        @Parameter(description = "pagination offset", example = "1000") @DefaultValue("0") @QueryParam("offset") @Min(0) int offset,
        @Context SecurityContext securityContext) {
        String opName = "getRecentTransferTasks";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            List<TransferTask> tasks = transfersService.getRecentTransfers(user.getTenantId(), user.getName(), limit, offset);
            TapisResponse<List<TransferTask>> resp = TapisResponse.createSuccessResponse(tasks);
            return Response.ok(resp).build();
        } catch (ServiceException e) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, opName, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }


    @GET
    @Path("/{transferTaskId}/")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get a transfer task", tags={ "transfers" })
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


        String opName = "getTransferTask";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();

        try {
            UUID transferTaskUUID = UUID.fromString(transferTaskId);
            TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
            if (task == null) {
                throw new NotFoundException(Utils.getMsgAuth("FILES_TXFR_NOT_FOUND", user, transferTaskUUID));
            }
            isPermitted(task, user);
            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
            return Response.ok(resp).build();
        } catch (ServiceException e) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, opName, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

    @GET
    @Path("/{transferTaskId}/details/")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get details of a transfer task", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = TransferTaskResponse.class))
            )
    })
    public Response getTransferTaskDetails(
            @ValidUUID
            @Parameter(description = "Transfer task ID", required=true, example = EXAMPLE_TASK_ID)
            @PathParam("transferTaskId") String transferTaskId,
            @Context SecurityContext securityContext) {


        String opName = "getTransferTaskHistory";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            UUID transferTaskUUID = UUID.fromString(transferTaskId);
            TransferTask task = transfersService.getTransferTaskDetails(transferTaskUUID);
            if (task == null) {
                throw new NotFoundException(Utils.getMsgAuth("FILES_TXFR_NOT_FOUND", user, transferTaskUUID));
            }
            isPermitted(task, user);
            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
            return Response.ok(resp).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }


    @DELETE
    @Path("/{transferTaskId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Stop/Cancel a transfer task", tags={ "transfers" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = StringResponse.class))
            )
    })
    public Response cancelTransferTask(
            @ValidUUID @Parameter(description = "Transfer task ID",required=true, example = EXAMPLE_TASK_ID) @PathParam("transferTaskId") String transferTaskId,
            @Context SecurityContext securityContext) {

        String opName = "cancelTransferTask";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();

        try {
            UUID transferTaskUUID = UUID.fromString(transferTaskId);
            TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
            if (task == null) {
                throw new NotFoundException(Utils.getMsgAuth("FILES_TXFR_NOT_FOUND", user, transferTaskUUID));
            }
            isPermitted(task, user);
            transfersService.cancelTransfer(task);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse(null);
            resp.setMessage("Transfer deleted.");
            return Response.ok(resp).build();
        } catch (ServiceException e) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, opName, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
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
        log.debug("TRANSFER CREATING");
        log.debug(transferTaskRequest.toString());

        String opName = "createTransferTask";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            TransferTask task = transfersService.createTransfer(
                    user.getOboUser(),
                    user.getOboTenantId(),
                    transferTaskRequest.getTag(),
                    transferTaskRequest.getElements()
            );
            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
            resp.setMessage("Transfer created.");
            log.debug("TRANSFER SAVED");
            log.debug(task.toString());
            return Response.ok(resp).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }
}
