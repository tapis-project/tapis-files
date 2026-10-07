package edu.utexas.tacc.tapis.files.api.resources;

import javax.inject.Inject;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import edu.utexas.tacc.tapis.files.api.FilesApplication;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ChildTaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ConnectionDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ParentTaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.StatusSummary;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TopTaskDetails;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.ManagementStatsService;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/*
 * JAX-RS REST resource for reporting Tapis Files worker/queue/connection status.
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *
 * getChildWorkerStatus, getQueuedTaskStatus and getSshSessionPoolStatus are placeholder
 * endpoints only - responses are not yet wired up to real status data.
 */
@Path("/v3/files/status")
public class ManagementStatsResource
{
  private static final Logger log = LoggerFactory.getLogger(ManagementStatsResource.class);
  private final String className = getClass().getSimpleName();

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  @Context
  private Request _request;

  @Inject
  private ManagementStatsService statsService;

  @Inject
  private FilePermsService filePermsService;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************
  @GET
  @Path("/summary")
  @Produces(MediaType.APPLICATION_JSON)
  public Response summaryStats(@QueryParam("limitPerType") @DefaultValue("1000") @Min(1) @Max(100000) int limitPerType,
                             @QueryParam("status") @DefaultValue("all") String statusString,
                             @Context SecurityContext securityContext)
  {
    String opName = "tasksStats";
    ResourceRequestUser rUser = checkAdminRequest(securityContext, opName);

    StatusSummary statusSummary = null;
    try {
      statusSummary = statsService.getStatusSummary();
    } catch (ServiceException e) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_ERROR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_COMPLETE", rUser, opName);
    TapisResponse<StatusSummary> resp = TapisResponse.createSuccessResponse(msg, statusSummary);
    return Response.ok(resp).build();
  }

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************
  @GET
  @Path("/tasks/top")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTopTaskDetails(@QueryParam("successLimit") @DefaultValue("100") @Min(1) @Max(100000) int successLimit,
                                    @QueryParam("errorLimit") @DefaultValue("100") @Min(1) @Max(100000) int errorLimit,
                                    @Context SecurityContext securityContext)
  {
    String opName = "tasksStats";
    ResourceRequestUser rUser = checkAdminRequest(securityContext, opName);

    TopTaskDetails topTaskDetails = null;
    try {
      topTaskDetails = statsService.getTopTaskDetails(successLimit, errorLimit);
    } catch (ServiceException e) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_ERROR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_COMPLETE", rUser, opName);
    TapisResponse<TopTaskDetails> resp = TapisResponse.createSuccessResponse(msg, topTaskDetails);
    return Response.ok(resp).build();
  }

  @GET
  @Path("/tasks/parent")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getParentTaskDetails(@QueryParam("limitPerType") @DefaultValue("1000") @Min(1) @Max(100000) int limitPerType,
                                 @Context SecurityContext securityContext)
  {
    String opName = "tasksStats";
    ResourceRequestUser rUser = checkAdminRequest(securityContext, opName);

    ParentTaskDetails parentTaskDetails = null;
    try {
      parentTaskDetails = statsService.getParentTaskDetails();
    } catch (ServiceException e) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_ERROR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_COMPLETE", rUser, opName);
    TapisResponse<ParentTaskDetails> resp = TapisResponse.createSuccessResponse(msg, parentTaskDetails);
    return Response.ok(resp).build();
  }

  @GET
  @Path("/tasks/child")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getChildTaskDetails(@QueryParam("limitPerType") @DefaultValue("1000") @Min(1) @Max(100000) int limitPerType,
                                 @Context SecurityContext securityContext)
  {
    String opName = "tasksStats";
    ResourceRequestUser rUser = checkAdminRequest(securityContext, opName);

    ChildTaskDetails childTaskDetails = null;
    try {
      childTaskDetails = statsService.getChildTaskDetails();
    } catch (ServiceException e) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_ERROR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_COMPLETE", rUser, opName);
    TapisResponse<ChildTaskDetails> resp = TapisResponse.createSuccessResponse(msg, childTaskDetails);
    return Response.ok(resp).build();
  }


  @GET
  @Path("/tasks/{topTaskIdOrUuid}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTaskDetails(@PathParam("topTaskIdOrUuid") String topTaskIdOrUuid,
                                 @QueryParam("parentLimit") @DefaultValue("100") @Min(1) @Max(100000) int parentLimit,
                                 @QueryParam("childLimit") @DefaultValue("100") @Min(1) @Max(100000) int childLimit,
                                 @Context SecurityContext securityContext)
  {
    String opName = "tasksStats";
    ResourceRequestUser rUser = checkAdminRequest(securityContext, opName);

    TaskDetails taskDetails = null;
    try {
      try {
        UUID taskUuid = UUID.fromString(topTaskIdOrUuid);
        taskDetails = statsService.getTaskDetailsByUuid(taskUuid, parentLimit, childLimit);
      } catch (IllegalArgumentException e) {
        int taskId = Integer.parseInt(topTaskIdOrUuid);
        taskDetails = statsService.getTaskDetailsByTaskId(taskId, parentLimit, childLimit);
      }

    } catch (ServiceException e) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_ERROR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_COMPLETE", rUser, opName);
    TapisResponse<TaskDetails> resp = TapisResponse.createSuccessResponse(msg, taskDetails);
    return Response.ok(resp).build();
  }

  @GET
  @Path("/sessions")
  @Produces(MediaType.APPLICATION_JSON)
  public Response sessionStats(@Context SecurityContext securityContext)
  {
    String opName = "connectionStats";
    ResourceRequestUser rUser = checkAdminRequest(securityContext, opName);

    List<ConnectionDetails> connectionDetails = null;
    try {
      connectionDetails = statsService.getConnectionInfo();
    } catch (ServiceException e) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_ERROR", rUser, opName, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_STATUS_OP_COMPLETE", rUser, opName);
    TapisResponse<List<ConnectionDetails>> resp = TapisResponse.createSuccessResponse(msg, connectionDetails);
    return Response.ok(resp).build();
  }

  // ************************************************************************
  // *********************** Private Methods **********************************
  // ************************************************************************

  /**
   * Common request validation shared by all status endpoints: context check, trace logging,
   * and service-restriction check.
   * @return null if OK, otherwise an error Response to return directly.
   */
  private Response checkRequest(SecurityContext securityContext, String opName)
  {
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext);
    if (resp1 != null) return resp1;

    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    if (log.isTraceEnabled()) {
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString());
    }

    TapisRestUtils.checkServiceRestrictions(TapisConstants.SERVICE_NAME_FILES, FilesApplication.getTrustedServices(), rUser);
    return null;
  }

  /**
   * Same as checkRequest(), but additionally requires that the caller be a tenant admin. Used by
   * endpoints (such as getParentWorkerStatus) that report cross-tenant/cross-user information.
   * Throws WebApplicationException (via checkContext) or ForbiddenException on failure.
   * @return the ResourceRequestUser for the caller, once all checks have passed.
   */
  private ResourceRequestUser checkAdminRequest(SecurityContext securityContext, String opName)
  {
    Response resp1 = checkRequest(securityContext, opName);
    if (resp1 != null) throw new WebApplicationException(resp1);

    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    boolean isPermitted;
    try {
      isPermitted = filePermsService.isSiteAdmin(rUser.getOboTenantId(), rUser.getOboUserId());
    } catch (ServiceException e) {
      // Fail closed - if we can't determine admin status, treat the caller as not authorized.
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_PERMISSION_REQUIRED", rUser, opName);
      log.error(msg, e);
      throw new ForbiddenException(msg);
    }

    if (!isPermitted) {
      String msg = ApiUtils.getMsgAuth("FAPI_STATUS_PERMISSION_REQUIRED", rUser, opName);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
    return rUser;
  }

}
