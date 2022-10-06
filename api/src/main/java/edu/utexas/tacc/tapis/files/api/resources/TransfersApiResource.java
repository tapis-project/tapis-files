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

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_IMPERSONATE;

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
      TapisResponse<List<TransferTask>> resp = TapisResponse.createSuccessResponse(tasks);
      return Response.ok(resp).build();
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
      isPermitted(task, rUser.getOboUserId(), rUser.getOboTenantId());
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      return Response.ok(resp).build();
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

      isPermitted(task, oboOrImpersonatedUser, rUser.getOboTenantId());
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      return Response.ok(resp).build();
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
      isPermitted(task, rUser.getOboUserId(), rUser.getOboTenantId());
      transfersService.cancelTransfer(task);
      TapisResponse<String> resp = TapisResponse.createSuccessResponse(null);
      resp.setMessage("Transfer deleted.");
      return Response.ok(resp).build();
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "txfrTaskRequestTag="+transferTaskRequest.getTag());

    // ---------------------------- Make service call -------------------------------
    try
    {
      // Create the txfr task
      TransferTask task = transfersService.createTransfer(rUser, transferTaskRequest.getTag(), transferTaskRequest.getElements());
      TapisResponse<TransferTask> resp = TapisResponse.createSuccessResponse(task);
      resp.setMessage("Transfer created.");
      // Trace details of the created txfr task.
      if (log.isTraceEnabled()) log.trace(task.toString());

      return Response.ok(resp).build();
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
   * @param task - task to check
   * @param oboUser - user trying to act on the task
   * @throws NotAuthorizedException if not authorized
   */
  private void isPermitted(TransferTask task, String oboUser, String oboTenant) throws NotAuthorizedException
  {
    if (!task.getUsername().equals(oboUser)) throw new NotAuthorizedException("");
    if (!task.getTenantId().equals(oboTenant)) throw new NotAuthorizedException("");
  }
}
