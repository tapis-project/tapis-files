package edu.utexas.tacc.tapis.files.api.resources;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.glassfish.grizzly.http.server.Request;
import org.apache.commons.io.IOUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.api.responses.RespShareInfo;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;

/*
 * JAX-RS REST resource for Tapis File sharing operations
 *
 * These methods should do the minimal amount of validation and processing of incoming requests and
 *   then make the service method call.
 * One reason for this is the service methods are much easier to test. Also, it results in easier to follow code.
 *
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 * Annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *
 * NOTE: Paths stored in SK for permissions and shares always relative to rootDir and always start with /
 * Shares are stored in the Security Kernel
 */
@Path("/v3/files")
public class ShareResource
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(ShareResource.class);
  private final String className = getClass().getSimpleName();
  // Always return a nicely formatted response
  private static final boolean PRETTY = true;
  // Json schema resource files.
  private static final String FILE_SHARE_REQUEST = "/edu/utexas/tacc/tapis/files/api/jsonschema/ShareRequest.json";
  // Field names used in Json
  private static final String USERLIST_FIELD = "users";

  private static final String OP_SHARE_PATH_USERS = "sharePath";
  private static final String OP_UNSHARE_PATH_USERS = "unSharePath";
  private static final String OP_SHARE_PATH_PUBLIC = "sharePathPublic";
  private static final String OP_UNSHARE_PATH_PUBLIC = "unSharePathPublic";

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
  private ServletContext _servletContext;
  @Context
  private Request _request;

  // **************** Inject Services using HK2 ****************
  @Inject
  private FileShareService svc;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Get share info for path.
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return response containing share info
   */
  @GET
  @Path("/share/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response getShareInfo(@PathParam("systemId") String systemId,
                               @PathParam("path") String path,
                               @Context SecurityContext securityContext)
  {
    String opName = "getShareInfo";
    return getShares(opName, systemId, path, securityContext);
  }

  @GET
  @Path("/share/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getShareInfoRoot(@PathParam("systemId") String systemId,
                                   @Context SecurityContext securityContext)
  {
    String opName = "getShareInfoRoot";
    return getShares(opName, systemId, "", securityContext);
  }

  /**
   * Share a path with one or more users
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param payloadStream - request body
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Path("/share/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response sharePath(InputStream payloadStream,
                            @PathParam("systemId") String systemId,
                            @PathParam("path") String path,
                            @Context SecurityContext securityContext)
  {
    return postUpdateUserShares(OP_SHARE_PATH_USERS, systemId, path, payloadStream, securityContext);
  }

  @POST
  @Path("/share/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response sharePathRoot(InputStream payloadStream,
                                @PathParam("systemId") String systemId,
                                @Context SecurityContext securityContext)
  {
    return postUpdateUserShares(OP_SHARE_PATH_USERS, systemId, "", payloadStream, securityContext);
  }

  /**
   * Share a path on a system publicly with all users in the tenant.
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Path("/share_public/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response sharePathPublic(@PathParam("systemId") String systemId,
                                  @PathParam("path") String path,
                                  @Context SecurityContext securityContext)
  {
    return postUpdatePublicShare(OP_SHARE_PATH_PUBLIC, systemId, path, securityContext);
  }

  @POST
  @Path("/share_public/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response sharePathPublicRoot(@PathParam("systemId") String systemId,
                                      @Context SecurityContext securityContext)
  {
    return postUpdatePublicShare(OP_SHARE_PATH_PUBLIC, systemId, "", securityContext);
  }

  /**
   * Unshare a path with one or more users
   *
   * @param payloadStream - request body
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Path("/unshare/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePath(InputStream payloadStream,
                              @PathParam("systemId") String systemId,
                              @PathParam("path") String path,
                              @Context SecurityContext securityContext)
  {
    return postUpdateUserShares(OP_UNSHARE_PATH_USERS, systemId, path, payloadStream, securityContext);
  }

  @POST
  @Path("/unshare/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePathRoot(InputStream payloadStream,
                                  @PathParam("systemId") String systemId,
                                  @Context SecurityContext securityContext)
  {
    return postUpdateUserShares(OP_UNSHARE_PATH_USERS, systemId, "", payloadStream, securityContext);
  }

  /**
   * Remove public access for a path on a system.
   *
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Path("/unshare_public/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePathPublic(@PathParam("systemId") String systemId,
                                    @PathParam("path") String path,
                                    @Context SecurityContext securityContext)
  {
    return postUpdatePublicShare(OP_UNSHARE_PATH_PUBLIC, systemId, path, securityContext);
  }

  @POST
  @Path("/unshare_public/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePathPublicRoot(@PathParam("systemId") String systemId,
                                        @Context SecurityContext securityContext)
  {
    return postUpdatePublicShare(OP_UNSHARE_PATH_PUBLIC, systemId, "", securityContext);
  }

  /**
   * Remove all shares for a path and all of sub-paths. Will also remove public share.
   *
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param recurse - if true include all sub-paths as well
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Path("/unshare_all/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePathAll(@PathParam("systemId") String systemId,
                                 @PathParam("path") String path,
                                 @QueryParam("recurse") @DefaultValue("false") boolean recurse,
                                 @Context SecurityContext securityContext)
  {
    String opName = "unSharePathAll";
    return removeAllShares(opName, systemId, path, recurse, securityContext);
  }

  @POST
  @Path("/unshare_all/{systemId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePathAll(@PathParam("systemId") String systemId,
                                 @QueryParam("recurse") @DefaultValue("false") boolean recurse,
                                 @Context SecurityContext securityContext)
  {
    String opName = "unSharePathAllRoot";
    return removeAllShares(opName, systemId, "", recurse, securityContext);
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Check json payload for a share/unshare request and extract set of users
   *
   * @param systemId - name of the system, for constructing response msg
   * @param path - name of path associated with the request, for constructing response msg
   * @param json - Request json extracted from payloadStream
   * @param userList - Set to be populated with users extracted from payload
   * @return - null if all checks OK else Response containing info
   */
  private Response checkAndExtractPayload(ResourceRequestUser rUser, String systemId, String path,
                                          String json, Set<String> userList, String op)
  {
    String msg;
    // Create validator specification and validate the json against the schema
    JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SHARE_REQUEST);
    try { JsonValidator.validate(spec); }
    catch (TapisJSONException e)
    {
      msg = ApiUtils.getMsgAuth("FAPI_SHARE_JSON_INVALID", rUser, op, systemId, path, e.getMessage());
      log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    JsonObject obj = TapisGsonUtils.getGson().fromJson(json, JsonObject.class);

    // Extract permissions from the request body
    JsonArray users = null;
    if (obj.has(USERLIST_FIELD)) users = obj.getAsJsonArray(USERLIST_FIELD);
    if (users != null && users.size() > 0)
    {
      for (int i = 0; i < users.size(); i++)
      {
        // Remove quotes from around incoming string
        String userStr = StringUtils.remove(users.get(i).toString(),'"');
        userList.add(userStr);
      }
    }
    // We require at least one user
    if (users == null || users.size() <= 0)
    {
      msg = ApiUtils.getMsgAuth("FAPI_SHARE_NOUSERS", rUser, op, systemId, path);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }
    return null;
  }

  /*
   * Common routine to update share/unshare for a list of users.
   */
  private Response postUpdateUserShares(String opName, String systemId, String path, InputStream payloadStream,
                                        SecurityContext securityContext)
  {
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId, "path="+path);

    // Read the payload into a string.
    String json, msg;
    try { json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("FAPI_SHARE_JSON_ERROR", rUser, opName, systemId, path, e.getMessage());
      log.error(msg, e);
      return Response.status(Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ------------------------- Extract and validate payload -------------------------
    var userSet = new HashSet<String>();
    resp = checkAndExtractPayload(rUser, systemId, path, json, userSet, opName);
    if (resp != null) return resp;

    // ------------------------- Perform the operation -------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    switch (opName)
    {
      case OP_SHARE_PATH_USERS   -> svc.sharePath(rUser, systemId, path, userSet);
      case OP_UNSHARE_PATH_USERS -> svc.unSharePath(rUser, systemId, path, userSet);
    }

    // ---------------------------- Success -------------------------------
    String userListStr = String.join(",", userSet);
    RespBasic resp1 = new RespBasic();
    msg = ApiUtils.getMsgAuth("FAPI_SHARE_U_UPDATED", rUser, opName, systemId, path, userListStr);
    log.info(msg);
    return Response.status(Status.OK)
            .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
            .build();
  }

  /*
   * Common routine to update public sharing.
   */
  private Response postUpdatePublicShare(String opName, String systemId, String path, SecurityContext securityContext)
  {
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId, "path="+path);

    // ------------------------- Perform the operation -------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    switch (opName)
    {
      case OP_SHARE_PATH_PUBLIC   -> svc.sharePathPublic(rUser, systemId, path);
      case OP_UNSHARE_PATH_PUBLIC -> svc.unSharePathPublic(rUser, systemId, path);
    }

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    String msg = ApiUtils.getMsgAuth("FAPI_SHARE_P_UPDATED", rUser, opName, systemId, path);
    log.info(msg);
    return Response.status(Status.OK)
            .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
            .build();
  }

  /*
   * Common routine to get share info
   */
  private Response getShares(String opName, String systemId, String path, SecurityContext securityContext)
  {
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    // If there is a problem return error response
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId, "path="+path);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    ShareInfo shareInfo = svc.getShareInfo(rUser, systemId, path);

    // No share info for path.
    if (shareInfo == null)
    {
      String msg = ApiUtils.getMsgAuth("FAPI_SHARE_NOT_FOUND", rUser, systemId, path);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the information.
    RespShareInfo resp1 = new RespShareInfo(shareInfo);
    String msg = ApiUtils.getMsg("FAPI_SHARE_FOUND", systemId, path);
    return Response.status(Status.OK)
            .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
            .build();
  }

  /*
   * Common routine to remove all shares
   */
  private Response removeAllShares(String opName, String systemId, String path, boolean recurse,
                                   SecurityContext securityContext)
  {
    // ------------------------- Retrieve and validate thread context -------------------------
    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
    if (resp != null) return resp;

    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(),
              "systemId="+systemId, "path="+path, "recurse="+recurse);

    // ------------------------- Perform the operation -------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    svc.removeAllSharesForPath(rUser, systemId, path, recurse);

    // ---------------------------- Success -------------------------------
    RespBasic resp1 = new RespBasic();
    String msg = ApiUtils.getMsgAuth("FAPI_SHARE_DEL_ALL", rUser, systemId, path);
    log.info(msg);
    return Response.status(Status.OK)
            .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
            .build();
  }
}