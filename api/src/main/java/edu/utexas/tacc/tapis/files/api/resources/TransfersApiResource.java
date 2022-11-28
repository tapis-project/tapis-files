package edu.utexas.tacc.tapis.files.api.resources;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.validators.ValidUUID;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_IMPERSONATE;

/*
 * JAX-RS REST resource for Tapis file transfer operations
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
@Path("/v3/files/transfers")
public class  TransfersApiResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  private static final Logger log = LoggerFactory.getLogger(TransfersApiResource.class);
  private final String className = getClass().getSimpleName();
  // Format strings
  private static final String TASKS_CNT_STR = "%d transfer tasks";

  private static final String FILES_SVC = StringUtils.capitalize(TapisConstants.SERVICE_NAME_FILES);

  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

  // Message keys
  private static final String TAPIS_FOUND = "TAPIS_FOUND";

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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "limit="+limit, "offset="+offset);

    try
    {
      List<TransferTask> tasks = transfersService.getRecentTransfers(rUser.getOboTenantId(), rUser.getOboUserId(), limit, offset);
      if (tasks == null) tasks = Collections.emptyList();
      RespBasic resp = new RespBasic(tasks);
      String itemCountStr = String.format(TASKS_CNT_STR, tasks.size());
      return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, FILES_SVC, itemCountStr), resp);
// TODO remove
//      String msg = MsgUtils.getMsg("TAPIS_FOUND", "Transfer tasks", tasks.size(), " items");
//      return Response.status(Status.OK).entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp)).build();
    }
    catch (ServiceException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_TXFR_ERR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  @GET
  @Path("/{transferTaskId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTransferTask(@PathParam("transferTaskId") @ValidUUID String transferTaskId,
                                  @Context SecurityContext securityContext)
  {
    String opName = "getTransferTask";
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "transferTaskId="+transferTaskId);

    try
    {
      UUID transferTaskUUID = UUID.fromString(transferTaskId);
      TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
      if (task == null) throw new NotFoundException(LibUtils.getMsgAuthR("FILES_TXFR_NOT_FOUND", rUser, transferTaskUUID));
      isPermitted(task, rUser.getOboUserId(), rUser.getOboTenantId(), opName);

      RespBasic resp = new RespBasic(task);
      return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "TransferTask", transferTaskId), resp);
    }
    catch (ServiceException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_TXFR_ERR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  /**
   *
   * @param transferTaskId Id of transfer task.
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, getSystem (effUserId)
   * @param securityContext - user identity
   * @return response containing transfer task history.
   */
  @GET
  @Path("/{transferTaskId}/details")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTransferTaskDetails(@PathParam("transferTaskId") @ValidUUID String transferTaskId,
                                         @QueryParam("impersonationId") String impersonationId,
                                         @Context SecurityContext securityContext)
  {
    String opName = "getTransferTaskHistory";
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "transferTaskId="+transferTaskId,
                          "impersonationId="+impersonationId);

    try
    {
      UUID transferTaskUUID = UUID.fromString(transferTaskId);
      TransferTask task = transfersService.getTransferTaskDetails(transferTaskUUID);
      if (task == null) throw new NotFoundException(LibUtils.getMsgAuthR("FILES_TXFR_NOT_FOUND", rUser, transferTaskUUID));

      // Check permission taking into account impersonationId
      // Must be a service to use impersonationId
      if (!StringUtils.isBlank(impersonationId))
      {
        // If a service request the username will be the service name. E.g. systems, jobs, streams, etc
        String svcName = rUser.getJwtUserId();
        if (!rUser.isServiceRequest() || !SVCLIST_IMPERSONATE.contains(svcName))
        {
          String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_IMPERSONATE_TXFR", rUser, transferTaskId, impersonationId);
          throw new ForbiddenException(msg);
        }
        // An allowed service is impersonating, log it
        log.info(LibUtils.getMsgAuthR("FILES_AUTH_IMPERSONATE_TXFR", rUser, transferTaskId, impersonationId));
      }

      // Finally, check for perm using oboUser or impersonationId
      // Certain services are allowed to impersonate an OBO user for the purposes of authorization
      String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;

      isPermitted(task, oboOrImpersonatedUser, rUser.getOboTenantId(), opName);
      RespBasic resp = new RespBasic(task);
      return createSuccessResponse(Status.OK, MsgUtils.getMsg(TAPIS_FOUND, "TransferTaskDetails", transferTaskId), resp);
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_TXFR_ERR", rUser, opName, ex.getMessage());
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "transferTaskId="+transferTaskId);

    try
    {
      UUID transferTaskUUID = UUID.fromString(transferTaskId);
      TransferTask task = transfersService.getTransferTaskByUUID(transferTaskUUID);
      if (task == null) throw new NotFoundException(LibUtils.getMsgAuthR("FILES_TXFR_NOT_FOUND", rUser, transferTaskUUID));
      isPermitted(task, rUser.getOboUserId(), rUser.getOboTenantId(), opName);
      transfersService.cancelTransfer(task);

      RespBasic respBasic = new RespBasic();
      String msg = ApiUtils.getMsgAuth("FAPI_TXFR_CANCELLED", rUser, transferTaskId);
      return createSuccessResponse(Status.OK, msg, respBasic);
    }
    catch (ServiceException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_TXFR_ERR", rUser, opName, e.getMessage());
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
    String opName = "createTransferTask";
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "txfrTaskRequest="+transferTaskRequest);

    // ---------------------------- Make service call -------------------------------
    try
    {
      // Create the txfr task
      TransferTask task = transfersService.createTransfer(rUser, transferTaskRequest.getTag(), transferTaskRequest.getElements());
      RespBasic respBasic = new RespBasic(task);
      String msg = ApiUtils.getMsgAuth("FAPI_TXFR_CREATED", rUser, task.getUuid());
      // Trace details of the created txfr task.
      if (log.isTraceEnabled()) log.trace(task.toString());
      return createSuccessResponse(Status.CREATED, msg, respBasic);
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_TXFR_ERR", rUser, opName, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Check that user has permission to access and act on the task
   * Permitted only if task tenant+user matched obo tenant+user
   * @param task - task to check
   * @param oboUser - user trying to act on the task
   */
  private void isPermitted(TransferTask task, String oboUser, String oboTenant, String opName)
  {
    if (task.getTenantId().equals(oboTenant) && task.getUsername().equals(oboUser)) return;
    throw new ForbiddenException(ApiUtils.getMsg("FAPI_TASK_UNAUTH", oboTenant, oboUser, task.getTenantId(),
                                                 task.getUsername(), task.getUuid(), opName));
  }

  /**
   * Create an OK response given message and base response to put in result
   * @param msg - message for resp.message
   * @param resp - base response (the result)
   * @return - Final response to return to client
   */
  private static Response createSuccessResponse(Response.Status status, String msg, RespAbstract resp)
  {
    return Response.status(status).entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp)).build();
  }
}
