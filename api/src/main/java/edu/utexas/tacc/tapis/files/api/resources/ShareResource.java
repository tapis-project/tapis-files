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
import edu.utexas.tacc.tapis.files.api.responses.RespShareInfo;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.glassfish.grizzly.http.server.Request;
import org.apache.commons.io.IOUtils;

import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;

/*
 * JAX-RS REST resource for Tapis File sharing operations
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 * Annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *
 * Shares are stored in the Security Kernel
 */
@Path("/v3/files/share")
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

  // **************** Inject Services using HK2 ****************
  @Inject
  FileShareService svc;

  @Inject
  SystemsCache systemsCache;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Share a path with one or more users
   * @param payloadStream - request body
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response sharePath(InputStream payloadStream,
                            @PathParam("systemId") String systemId,
                            @PathParam("path") String path,
                            @Context SecurityContext securityContext)
  {
    String opName = "sharePath";
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

    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // Read the payload into a string.
    String json;
    String msg;
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
    //   to responses (FilesExceptionMapper).
    svc.sharePath(rUser, sys, path, userSet, json);

    // ---------------------------- Success -------------------------------
    String userListStr = String.join(",", userSet);
    RespBasic resp1 = new RespBasic();
    msg = ApiUtils.getMsgAuth("FAPI_SHARE_UPDATED", rUser, opName, systemId, path, userListStr);
    return Response.status(Status.CREATED)
      .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
      .build();
  }

  /**
   * Unshare a path with one or more users
   * @param payloadStream - request body
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return basic response
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response unSharePath(InputStream payloadStream,
                              @PathParam("systemId") String systemId,
                              @PathParam("path") String path,
                              @Context SecurityContext securityContext)
  {
    String opName = "unSharePath";
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

    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // Read the payload into a string.
    String json;
    String msg;
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
    //   to responses (FilesExceptionMapper).
    svc.unSharePath(rUser, sys, path, userSet, json);

    // ---------------------------- Success -------------------------------
    String userListStr = String.join(",", userSet);
    RespBasic resp1 = new RespBasic();
    msg = ApiUtils.getMsgAuth("FAPI_SHARE_UPDATED", rUser, opName, systemId, path, userListStr);
    return Response.status(Status.CREATED)
            .entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp1))
            .build();
  }

  /**
   * Get share info for path.
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param securityContext - user identity
   * @return response containing share info
   */
  @GET
  @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response getShareInfo(@PathParam("systemId") String systemId,
                               @PathParam("path") String path,
                               @Context SecurityContext securityContext)
  {
    String opName = "getShareInfo";
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

    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // ---------------------------- Make service call -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (FilesExceptionMapper).
    ShareInfo shareInfo = svc.getShareInfo(rUser, sys, path);

    // No share info for path.
    if (shareInfo == null)
    {
      String msg = ApiUtils.getMsgAuth("FAPI_SHARE_NOT_FOUND", rUser, systemId, path);
      return Response.status(Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, PRETTY)).build();
    }

    // ---------------------------- Success -------------------------------
    // Success means we retrieved the information.
    RespShareInfo resp1 = new RespShareInfo(shareInfo);
    return createSuccessResponse(Status.OK, MsgUtils.getMsg("FAPI_SHARE_FOUND", systemId, path), resp1);
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /**
   * Check json payload for a share/unshare request and extract set of users
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

  /**
   * Create an OK response given message and base response to put in result
   * @param msg - message for resp.message
   * @param resp - base response (the result)
   * @return - Final response to return to client
   */
  private static Response createSuccessResponse(Status status, String msg, RespAbstract resp)
  {
    return Response.status(status).entity(TapisRestUtils.createSuccessResponse(msg, PRETTY, resp)).build();
  }
}