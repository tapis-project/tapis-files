package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.validators.ValidUUID;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.commons.lang3.StringUtils;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;


@Path("/v3/files/transfers")
public class  TransfersApiResource {

    private static final String EXAMPLE_TASK_ID = "6491c2a5-acb2-40ef-b2c0-bc1fc4cd7e6c";
    private static final Logger log = LoggerFactory.getLogger(TransfersApiResource.class);

    @Inject
    TransfersService transfersService;

    @Inject
    SystemsCache systemsCache;

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
        log.info("TRANSFER CREATING");
        log.info(transferTaskRequest.toString());

        String opName = "createTransferTask";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        // Make sure source and destination systems exist and are enabled
        Response response = validateSystems(transferTaskRequest, user);
        if (response != null) return response;

        try {
            // Create the txfr task
            TransferTask task = transfersService.createTransfer(
                    user.getOboUser(),
                    user.getOboTenantId(),
                    transferTaskRequest.getTag(),
                    transferTaskRequest.getElements()
            );
            TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
            resp.setMessage("Transfer created.");
            log.info("TRANSFER SAVED");
            log.info(task.toString());
            return Response.ok(resp).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_TXFR_ERR", user, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }

  // ======================================================================
  // ============= Private methods ========================================
  // ======================================================================

  /**
   * Check that all source and destination systems referenced in a TransferRequest exist and are enabled.
   *
   * @param transferTaskRequest - Request to check
   * @param user - AuthenticatedUser, contains user info needed to fetch systems
   * @return null if all OK, BAD_REQUEST response if there are any missing or disabled systems
   */
  private Response validateSystems(TransferTaskRequest transferTaskRequest, AuthenticatedUser user)
  {
    var errMessages = new ArrayList<String>();
    // If no transferTask elements we are done
    if (transferTaskRequest.getElements().isEmpty()) return null;
    // Collect sets of src and dest systems
    var allSystems = new HashSet<String>();
    for (TransferTaskRequestElement txfrElement : transferTaskRequest.getElements())
    {
      String srcId = txfrElement.getSourceURI().getSystemId();
      String dstId = txfrElement.getDestinationURI().getSystemId();
      if (!StringUtils.isBlank(srcId)) allSystems.add(srcId);
      if (!StringUtils.isBlank(dstId)) allSystems.add(dstId);
    }

    // Collect and log a list of any errors
    for (String sysId : allSystems) { validateSystemForTxfr(sysId, user, errMessages); }
    // If we have any errors log a message and return BAD_REQUEST response.
    if (!errMessages.isEmpty()) {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(user, transferTaskRequest.getTag(), errMessages);
      log.error(allErrors);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(allErrors, true)).build();
    }
    return null;
  }

  private void validateSystemForTxfr(String sysId, AuthenticatedUser authUser, List<String> errMessages)
  {
    TapisSystem system = null;
    try {
      system = systemsCache.getSystem(authUser.getOboTenantId(), sysId, authUser.getOboUser());
    } catch (ServiceException se) {
      // Unable to locate system
      errMessages.add(Utils.getMsg("FILES_TXFR_SYS_MISSING",sysId));
    }
    try {
      if (system != null) Utils.checkEnabled(authUser, system);
    } catch (BadRequestException bre) {
      // System not enabled.
      errMessages.add(Utils.getMsg("FILES_TXFR_SYS_DISABLED",sysId));
    }
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(AuthenticatedUser user, String txfrTaskTag, List<String> msgList) {
    var sb = new StringBuilder(Utils.getMsgAuth("FILES_TXFR_ERRORLIST", user, txfrTaskTag));
    sb.append(System.lineSeparator());
    if (msgList == null || msgList.isEmpty()) return sb.toString();
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }
}
