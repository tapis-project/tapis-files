package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.validators.ValidUUID;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
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
  private final String className = getClass().getSimpleName();

  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  @Context
  private HttpHeaders _httpHeaders;
  @Context
  private Application _application;
  @Context
  private UriInfo _uriInfo;
  @Context
  private SecurityContext _securityContext;
  @Context
  private ServletContext _servletContext;
  @Context
  private Request _request;

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
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

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
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

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
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

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
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

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
    String opName = "createTransferTask";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "txfrTaskRequestTag="+transferTaskRequest.getTag());

    // Make sure source and destination systems exist, are enabled, and we support transfers between the systems.
    resp1 = validateSystems(transferTaskRequest, rUser);
    if (resp1 != null) return resp1;

    // ---------------------------- Make service call -------------------------------
    try
    {
      // Create the txfr task
      TransferTask task = transfersService.createTransfer(user.getOboUser(), user.getOboTenantId(),
                                                          transferTaskRequest.getTag(),
                                                          transferTaskRequest.getElements());
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      resp.setMessage("Transfer created.");
      log.info("TRANSFER SAVED");
      // Trace details of the created txfr task.
      if (log.isTraceEnabled()) log.trace(task.toString());

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
   * Check that all source and destination systems referenced in a TransferRequest exist and are enabled
   *   and that based on the src and dst system types we support the transfer
   *
   * @param transferTaskRequest - Request to check
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @return null if all OK, BAD_REQUEST response if there are any missing or disabled systems
   */
  private Response validateSystems(TransferTaskRequest transferTaskRequest, ResourceRequestUser rUser)
  {
    var txfrElements = transferTaskRequest.getElements();
    // If no transferTask elements we are done
    if (txfrElements.isEmpty()) return null;

    // Collect full set of systems involved
    var allSystems = new HashSet<String>();
    for (TransferTaskRequestElement txfrElement : txfrElements)
    {
      String srcId = txfrElement.getSourceURI().getSystemId();
      String dstId = txfrElement.getDestinationURI().getSystemId();
      if (!StringUtils.isBlank(srcId)) allSystems.add(srcId);
      if (!StringUtils.isBlank(dstId)) allSystems.add(dstId);
    }

    var errMessages = new ArrayList<String>();
    // Make sure each system exists and is enabled
    for (String sysId : allSystems) { validateSystemForTxfr(sysId, rUser, errMessages); }

    // Check that we support transfers between each pair of systems
    validateSystemsForTxfrSupport(txfrElements, rUser, errMessages);

    // If we have any errors log a message and return BAD_REQUEST response.
    if (!errMessages.isEmpty())
    {
      // Construct message reporting all errors
      String allErrors = getListOfErrors(rUser, transferTaskRequest.getTag(), errMessages);
      log.error(allErrors);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(allErrors, true)).build();
    }
    return null;
  }

  /**
   * Make sure system exists and is enabled
   * @param sysId system to check
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param errMessages - List where error message are being collected
   */
  private void validateSystemForTxfr(String sysId, ResourceRequestUser rUser, List<String> errMessages)
  {
    try
    {
      LibUtils.getSystemIfEnabled(rUser, systemsCache, sysId);
    }
    catch (NotFoundException e)
    {
      // Unable to locate system or it is not enabled
      errMessages.add(e.getMessage());
    }
  }

  /**
   * Make sure we support transfers between each pair of systems in a transfer request
   * @param txfrElements - List of transfer elements
   * @param rUser - AuthenticatedUser, contains user info needed to fetch systems
   * @param errMessages - List where error message are being collected
   */
  private void validateSystemsForTxfrSupport(List<TransferTaskRequestElement> txfrElements, ResourceRequestUser rUser,
                                             List<String> errMessages)
  {
    // Check each pair of systems
    for (TransferTaskRequestElement txfrElement : txfrElements)
    {
      String srcId = txfrElement.getSourceURI().getSystemId();
      String dstId = txfrElement.getDestinationURI().getSystemId();
      // Get each system. These should already be in the cache due to a previous check, see validateSystems()
      TapisSystem srcSys = null, dstSys = null;
      try
      {
        srcSys = systemsCache.getSystem(rUser.getOboTenantId(), srcId, rUser.getOboUserId());
        dstSys = systemsCache.getSystem(rUser.getOboTenantId(), dstId, rUser.getOboUserId());
      }
      catch (ServiceException e)
      {
        // In theory this will not happen due to previous check, see validateSystems()
        errMessages.add(e.getMessage());
      }
      // If one is GLOBUS and the other is not then we do not support it
      if (srcSys != null && dstSys != null)
      {
        if (
            (SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType()) && !SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType()))
             || (!SystemTypeEnum.GLOBUS.equals(srcSys.getSystemType()) && SystemTypeEnum.GLOBUS.equals(dstSys.getSystemType()))
           )
        {
          errMessages.add(LibUtils.getMsg("FILES_TXFR_GLOBUS_NOTSUPPORTED", txfrElement.getSourceURI().toString(),
                                          txfrElement.getDestinationURI().toString()));
        }
      }
    }
  }

  /**
   * Construct message containing list of errors
   */
  private static String getListOfErrors(ResourceRequestUser rUser, String txfrTaskTag, List<String> msgList)
  {
    var sb = new StringBuilder(LibUtils.getMsgAuthR("FILES_TXFR_ERRORLIST", rUser, txfrTaskTag));
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
