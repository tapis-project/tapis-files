package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.validators.ValidUUID;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
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

/*
 * JAX-RS REST resource for Tapis file transfer operations
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
@Path("/v3/files/transfers")
public class  TransfersApiResource
{
  private static final Logger log = LoggerFactory.getLogger(TransfersApiResource.class);

  @Inject
  TransfersService transfersService;

  @Inject
  SystemsCache systemsCache;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRecentTransferTasks(@QueryParam("limit") @DefaultValue("1000") @Max(1000) int limit,
                                         @QueryParam("offset") @DefaultValue("0") @Min(0) int offset,
                                         @Context SecurityContext securityContext)
  {
    String opName = "getRecentTransferTasks";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    try
    {
      List<TransferTask> tasks = transfersService.getRecentTransfers(user.getTenantId(), user.getName(), limit, offset);
      TapisResponse<List<TransferTask>> resp = TapisResponse.createSuccessResponse(tasks);
      return Response.ok(resp).build();
    }
    catch (ServiceException e)
    {
      String msg = LibUtils.getMsgAuth("FILES_TXFR_ERR", user, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  @GET
  @Path("/{transferTaskId}/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTransferTask(@PathParam("transferTaskId") @ValidUUID String transferTaskId,
                                  @Context SecurityContext securityContext)
  {
    String opName = "getTransferTask";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    try
    {
      UUID transferTaskUUID = UUID.fromString(transferTaskId);
      TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
      if (task == null) throw new NotFoundException(LibUtils.getMsgAuth("FILES_TXFR_NOT_FOUND", user, transferTaskUUID));
      isPermitted(task, user);
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      return Response.ok(resp).build();
    }
    catch (ServiceException e)
    {
      String msg = LibUtils.getMsgAuth("FILES_TXFR_ERR", user, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  @GET
  @Path("/{transferTaskId}/details/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTransferTaskDetails(@PathParam("transferTaskId") @ValidUUID String transferTaskId,
                                         @Context SecurityContext securityContext)
  {
    String opName = "getTransferTaskHistory";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    try
    {
      UUID transferTaskUUID = UUID.fromString(transferTaskId);
      TransferTask task = transfersService.getTransferTaskDetails(transferTaskUUID);
      if (task == null) throw new NotFoundException(LibUtils.getMsgAuth("FILES_TXFR_NOT_FOUND", user, transferTaskUUID));
      isPermitted(task, user);
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      return Response.ok(resp).build();
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuth("FILES_TXFR_ERR", user, opName, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
  }

  @DELETE
  @Path("/{transferTaskId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelTransferTask(@PathParam("transferTaskId") @ValidUUID String transferTaskId,
                                     @Context SecurityContext securityContext)
  {
    String opName = "cancelTransferTask";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    try
    {
      UUID transferTaskUUID = UUID.fromString(transferTaskId);
      TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
      if (task == null) throw new NotFoundException(LibUtils.getMsgAuth("FILES_TXFR_NOT_FOUND", user, transferTaskUUID));
      isPermitted(task, user);
      transfersService.cancelTransfer(task);
      TapisResponse<String> resp = TapisResponse.createSuccessResponse(null);
      resp.setMessage("Transfer deleted.");
      return Response.ok(resp).build();
    }
    catch (ServiceException e)
    {
      String msg = LibUtils.getMsgAuth("FILES_TXFR_ERR", user, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createTransferTask(@Valid TransferTaskRequest transferTaskRequest,
                                     @Context SecurityContext securityContext)
  {
    log.info("TRANSFER CREATING");
    log.info(transferTaskRequest.toString());

    String opName = "createTransferTask";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    // Make sure source and destination systems exist and are enabled
    Response response = validateSystems(transferTaskRequest, user);
    if (response != null) return response;
    try
    {
      // Create the txfr task
      TransferTask task = transfersService.createTransfer(user.getOboUser(), user.getOboTenantId(),
                                                          transferTaskRequest.getTag(),
                                                          transferTaskRequest.getElements());
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      resp.setMessage("Transfer created.");
      log.info("TRANSFER SAVED");
      log.info(task.toString());
      return Response.ok(resp).build();
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuth("FILES_TXFR_ERR", user, opName, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

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

  /**
   * Make sure system exists and is enabled
   * @param sysId system to check
   * @param authUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param errMessages - List where error message are being collected
   */
  private void validateSystemForTxfr(String sysId, AuthenticatedUser authUser, List<String> errMessages)
  {
    TapisSystem system = null;
    try
    {
      system = systemsCache.getSystem(authUser.getOboTenantId(), sysId, authUser.getOboUser());
    }
    catch (ServiceException se)
    {
      // Unable to locate system
      errMessages.add(LibUtils.getMsg("FILES_TXFR_SYS_MISSING",sysId));
    }
    try
    {
      if (system != null) LibUtils.checkEnabled(authUser, system);
    }
    catch (BadRequestException bre)
    {
      // System not enabled.
      errMessages.add(LibUtils.getMsg("FILES_TXFR_SYS_DISABLED",sysId));
    }
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(AuthenticatedUser user, String txfrTaskTag, List<String> msgList)
  {
    var sb = new StringBuilder(LibUtils.getMsgAuth("FILES_TXFR_ERRORLIST", user, txfrTaskTag));
    sb.append(System.lineSeparator());
    if (msgList == null || msgList.isEmpty()) return sb.toString();
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  /**
   * Check that user has permission to access and act on the task
   * @param task - task to check
   * @param user - user trying to act on the task
   * @throws NotAuthorizedException if not authorized
   */
  private void isPermitted(TransferTask task, AuthenticatedUser user) throws NotAuthorizedException
  {
    if (!task.getUsername().equals(user.getOboUser())) throw new NotAuthorizedException("");
    if (!task.getTenantId().equals(user.getOboTenantId())) throw new NotAuthorizedException("");
  }
}
