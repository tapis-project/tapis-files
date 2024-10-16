package edu.utexas.tacc.tapis.files.api.resources;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import edu.utexas.tacc.tapis.files.lib.services.FileListingOpts;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRequest;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_RECURSION;

/*
 * JAX-RS REST resource for Tapis File operations (list, mkdir, delete, moveCopy, upload)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
@Path("/v3/files/ops")
public class OperationsApiResource
{
  private static final Logger log = LoggerFactory.getLogger(OperationsApiResource.class);
  private final String className = getClass().getSimpleName();

  // Some methods do not support impersonationId
  private static final String impersonationIdNull = null;
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
  FileOpsService fileOpsService;

  /**
   * List files at path.
   *  If recursion is specified max depth is MAX_RECURSION = 20
   * This is broken into two methods with a common method to perform the operation because of problems with
   *   listing the "root" directory. The pattern "/{systemId}/{path:(.*+)}" does not work when requester
   *   attempts the operation on "/{systemId}"
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @param recurse - flag indicating a recursive listing should be provided (up to depth of 10)
   * @param pattern - wildcard (glob) pattern or regex used to filter results.  Regex must be prefixed with "regex:".
   *                Only results with file names that match the regex will be returned
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, getSystem (effUserId)
   * @param sharedCtx - Grantor for the case of a shared context.
   * @param securityContext - user identity
   * @return response containing list of files
   */
  @GET
  @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response listFiles(@PathParam("systemId") String systemId,
                            @PathParam("path") String path,
                            @QueryParam("limit") @DefaultValue("1000") @Max(1000) int limit,
                            @QueryParam("offset") @DefaultValue("0") @Min(0) long offset,
                            @QueryParam("recurse") @DefaultValue("false") boolean recurse,
                            @QueryParam("pattern") @DefaultValue("") String pattern,
                            @QueryParam("impersonationId") String impersonationId,
                            @QueryParam("sharedCtx") String sharedCtx,
                            @Context SecurityContext securityContext)
  {
    String opName = "listFiles";
    FileListingOpts.Builder listOptsBuilder = new FileListingOpts.Builder()
            .setPageSize(limit)
            .setItemOffset(offset)
            .setRecurse(recurse)
            .setPattern(pattern);
    return getListing(opName, systemId, path, listOptsBuilder.build(), impersonationId, sharedCtx, securityContext);
  }

  @GET
  @Path("/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listFilesRoot(@PathParam("systemId") String systemId,
                                @QueryParam("limit") @DefaultValue("1000") @Max(1000) int limit,
                                @QueryParam("offset") @DefaultValue("0") @Min(0) long offset,
                                @QueryParam("recurse") @DefaultValue("false") boolean recurse,
                                @QueryParam("pattern") @DefaultValue("") String pattern,
                                @QueryParam("impersonationId") String impersonationId,
                                @QueryParam("sharedCtx") String sharedCtx,
                                @Context SecurityContext securityContext)
  {
    String opName = "listFilesRoot";
    FileListingOpts.Builder listOptsBuilder = new FileListingOpts.Builder()
            .setPageSize(limit)
            .setItemOffset(offset)
            .setRecurse(recurse)
            .setPattern(pattern);
    return getListing(opName, systemId, "", listOptsBuilder.build(), impersonationId, sharedCtx, securityContext);
  }

  /**
   * Upload a file
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param fileInputStream - stream of data to place in the file
   * @param securityContext - user identity
   * @return response
   */
  @POST
  @Path("/{systemId}/{path:.+}")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public Response upload(@PathParam("systemId") String systemId,
                         @PathParam("path") String path,
                         @FormDataParam(value = "file") InputStream fileInputStream,
                         @Context SecurityContext securityContext)
  {
    String opName = "upload";
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
      ApiUtils.logRequest(rUser,className,opName,_request.getRequestURL().toString(),"systemId="+systemId,"path="+path);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    fileOpsService.upload(rUser, systemId, path, fileInputStream);
    String msg = ApiUtils.getMsgAuth("FAPI_OP_COMPLETE", rUser, opName, systemId, path);
    TapisResponse<String> resp = TapisResponse.createSuccessResponse(msg, null);
    return Response.ok(resp).build();
  }

  /**
   * Create a directory
   * @param systemId - id of system
   * @param mkdirRequest - request body containing a path relative to system rootDir
   * @param sharedCtx - Grantor for the case of a shared context.
   * @param securityContext - user identity
   * @return response
   */
  @POST
  @Path("/{systemId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response mkdir(@PathParam("systemId") String systemId,
                        @Valid MkdirRequest mkdirRequest,
                        @QueryParam("sharedCtx") String sharedCtx,
                        @Context SecurityContext securityContext)
  {
    String opName = "mkdir";
    // Create a user that collects together tenant, user and request information needed by service calls
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    ResourceRequestUser rUser = new ResourceRequestUser(user);
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp1 = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp1 != null) return resp1;

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
                          "sharedCtx="+sharedCtx, "path="+mkdirRequest.getPath());

    Instant start = Instant.now();
    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    fileOpsService.mkdir(rUser, systemId, mkdirRequest.getPath(), sharedCtx);
    String durationMsg = LibUtils.getMsgAuth("FILES_DURATION", user, opName, systemId, Duration.between(start, Instant.now()).toMillis());
    log.trace(durationMsg);
    String msg = ApiUtils.getMsgAuth("FAPI_OP_COMPLETE", rUser, opName, systemId, mkdirRequest.getPath());
    log.trace(msg);
    TapisResponse<String> resp = TapisResponse.createSuccessResponse(msg, null);
    return Response.ok(resp).build();
  }

  /**
   * Perform a move or copy operation
   * @param systemId - id of system
   * @param path - source path on system
   * @param mvCpReq - request body containing operation (MOVE/COPY) and target path
   * @param securityContext - user identity
   * @return response
   */
  @PUT
  @Path("/{systemId}/{path:.+}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response moveCopy(@PathParam("systemId") String systemId,
                           @PathParam("path") String path,
                           @Valid MoveCopyRequest mvCpReq,
                           @Context SecurityContext securityContext)
  {
    String opName = "moveCopy";
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
                          "op="+mvCpReq.getOperation(), "newPath="+mvCpReq.getNewPath());

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    fileOpsService.moveOrCopy(rUser, mvCpReq.getOperation(), systemId, path, mvCpReq.getNewPath());
    String msg = ApiUtils.getMsgAuth("FAPI_MVCP_COMPLETE", rUser, mvCpReq.getOperation(), systemId, path, mvCpReq.getNewPath());
    TapisResponse<String> resp = TapisResponse.createSuccessResponse(msg, null);
    return Response.ok(resp).build();
  }

  /**
   * Delete a path
   * @param systemId - id of system
   * @param path - path to delete
   * @param securityContext - user identity
   * @return response
   */
  @DELETE
  @Path("/{systemId}/{path:(.*+)}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response delete(@PathParam("systemId") String systemId,
                         @PathParam("path") String path,
                         @Context SecurityContext securityContext)
  {
    String opName = "delete";
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
      ApiUtils.logRequest(rUser,className,opName,_request.getRequestURL().toString(),"systemId="+systemId,"path="+path);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    fileOpsService.delete(rUser, systemId, path);
    String msg = ApiUtils.getMsgAuth("FAPI_OP_COMPLETE", rUser, opName, systemId, path);
    TapisResponse<String> resp = TapisResponse.createSuccessResponse(msg, null);
    return Response.ok(resp).build();
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /*
   * Common routine to perform a listing
   */
  private Response getListing(String opName, String systemId, String path, FileListingOpts fileListingOpts,
                              String impersonationId, String sharedCtx, SecurityContext securityContext)
  {
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
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId, "path="+path,
              "pageLimit="+fileListingOpts.getPattern(), "itemOffset="+fileListingOpts.getItemOffset(), "recurse="+fileListingOpts.isRecurse(),"impersonationId="+impersonationId,
              "sharedCtx="+sharedCtx);
    Instant start = Instant.now();
    List<FileInfo> listing;
    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    if (fileListingOpts.isRecurse()) listing = fileOpsService.lsRecursive(rUser, systemId, path, fileListingOpts, impersonationId, sharedCtx);
    else listing = fileOpsService.ls(rUser, systemId, path, fileListingOpts, impersonationId, sharedCtx);

    String msg = LibUtils.getMsgAuth("FILES_DURATION", user, opName, systemId, Duration.between(start, Instant.now()).toMillis());
    log.debug(msg);
    msg = ApiUtils.getMsgAuth("FAPI_OP_COMPLETE", rUser, opName, systemId, path);
    TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse(msg, listing);
    return Response.status(Status.OK).entity(resp).build();
  }
}
