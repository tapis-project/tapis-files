package edu.utexas.tacc.tapis.files.api.resources;

import java.util.List;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.api.models.NativeLinuxFaclRequest;
import edu.utexas.tacc.tapis.files.api.models.NativeLinuxOpRequest;
import edu.utexas.tacc.tapis.files.lib.models.AclEntry;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;

/*
 * JAX-RS REST resource for Tapis file native linux operations (stat, chmod, chown, chgrp)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 *
 * Another option would be to have systemType as a path parameter and rename this class to UtilsApiResource.
 */
@Path("/v3/files/utils/linux")
public class UtilsLinuxApiResource
{
  private static final Logger log = LoggerFactory.getLogger(UtilsLinuxApiResource.class);
  private final String className = getClass().getSimpleName();

  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  @Context
  private Request _request;

  @Inject
  FileUtilsService fileUtilsService;
  @Inject
  SystemsCache systemsCache;
  @Inject
  SystemsCacheNoAuth systemsCacheNoAuth;

  @GET
  @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatInfo(@PathParam("systemId") String systemId,
                              @PathParam("path") String path,
                              @QueryParam("followLinks") @DefaultValue("false") boolean followLinks,
                              @Context SecurityContext securityContext)
  {
    String opName = "getStatInfo";
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser,className,opName,_request.getRequestURL().toString(),
                          "systemId="+systemId,"path="+path,"followLinks="+followLinks);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    FileStatInfo fileStatInfo = fileUtilsService.getStatInfo(rUser, systemsCache, systemsCacheNoAuth, systemId, path, followLinks);

    String msg = ApiUtils.getMsgAuth("FAPI_OP_COMPLETE", rUser, opName, systemId, path);
    TapisResponse<FileStatInfo> resp = TapisResponse.createSuccessResponse(msg, fileStatInfo);
    return Response.ok(resp).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{systemId}/{path:.+}")
  public Response runLinuxNativeOp(@PathParam("systemId") String systemId,
                                   @PathParam("path") String path,
                                   @Valid NativeLinuxOpRequest request,
                                   @QueryParam("recursive") @DefaultValue("false") boolean recursive,
                                   @Context SecurityContext securityContext)
  {
    String opName = "runLinuxNativeOp";
    FileUtilsService.NativeLinuxOperation linuxOp = request.getOperation();
    String linuxOpArg = request.getArgument();
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

    // Trace this request.
    ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
                        "linuxOp="+linuxOp,"argument="+linuxOpArg,"path="+path,"recursive="+recursive);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    NativeLinuxOpResult opResult = fileUtilsService.runLinuxOp(rUser, systemsCache, systemsCacheNoAuth, systemId, path,
                                                               linuxOp, linuxOpArg, recursive);

    String msg = ApiUtils.getMsgAuth("FAPI_LINUX_OP_DONE", rUser, linuxOp.name(), systemId, path);
    TapisResponse<NativeLinuxOpResult> resp = TapisResponse.createSuccessResponse(msg, opResult);
    return Response.ok(resp).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("facl/{systemId}/{path:.*+}")
  public Response runLinuxGetfacl(@PathParam("systemId") String systemId,
                                   @PathParam("path") String path,
                                   @Context SecurityContext securityContext)
  {
    String opName = "runLinuxGetfacl";
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
                          "systemId="+systemId, "path="+path);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    List<AclEntry> aclEntries = fileUtilsService.runGetfacl(rUser, systemsCache, systemsCacheNoAuth, systemId, path);

    String msg = ApiUtils.getMsgAuth("FAPI_LINUX_OP_DONE", rUser, opName, systemId, path);
    TapisResponse<List<AclEntry>> resp = TapisResponse.createSuccessResponse(msg, aclEntries);
    return Response.ok(resp).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("facl/{systemId}/{path:.*+}")
  public Response runLinuxSetfacl(@PathParam("systemId") String systemId,
                                  @PathParam("path") String path,
                                  @Valid NativeLinuxFaclRequest request,
                                  @Context SecurityContext securityContext)
  {
    String opName = "runLinuxSetfacl";
    FileUtilsService.NativeLinuxFaclOperation faclOp = request.getOperation();
    FileUtilsService.NativeLinuxFaclRecursion recursionMethod = request.getRecursionMethod();
    var aclString = request.getAclString();
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser,className,opName,_request.getRequestURL().toString(),"systemId="+systemId,
                          "path="+path, "faclOp="+faclOp, "aclString="+aclString, "recursionMethod="+recursionMethod);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    NativeLinuxOpResult opResult = fileUtilsService.runSetfacl(rUser, systemsCache, systemsCacheNoAuth, systemId, path,
                                                               faclOp, recursionMethod, aclString);

    String msg = ApiUtils.getMsgAuth("FAPI_LINUX_OP_DONE", rUser, opName, systemId, path);
    TapisResponse<NativeLinuxOpResult> resp = TapisResponse.createSuccessResponse(msg, opResult);
    return Response.ok(resp).build();
  }
}
