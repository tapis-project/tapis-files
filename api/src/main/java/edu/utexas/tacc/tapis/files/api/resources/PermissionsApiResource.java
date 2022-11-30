package edu.utexas.tacc.tapis.files.api.resources;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.files.api.models.CreatePermissionRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthorization;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.models.FilePermission;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * JAX-RS REST resource for Tapis File permissions (Tapis permissions, not native permissions).
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 *
 * NOTE: Paths stored in SK for permissions and shares always relative to rootDir and always start with /
 */
@Path("/v3/files/permissions")
public class PermissionsApiResource
{
  private static final Logger log = LoggerFactory.getLogger(PermissionsApiResource.class);
  private final FilePermsService permsService;
  private final SystemsCache systemsCache;

  // **************** Inject Services using HK2 ****************
  @Inject
  public PermissionsApiResource(FilePermsService permsService, SystemsCache systemsCache)
  {
    this.permsService = permsService;
    this.systemsCache = systemsCache;
  }

  @DELETE
  @FilePermissionsAuthorization
  @Path("/{systemId}/{path:(.*+)}")
  @Produces({ MediaType.APPLICATION_JSON })
  public Response deletePermissions(@PathParam("systemId") String systemId,
                                    @PathParam("path") String path,
                                    @QueryParam("username") @NotEmpty String username,
                                    @Context SecurityContext securityContext) throws NotFoundException
  {
    String opName = "revokePermissions";
    path = StringUtils.isBlank(path) ? "/" : path;
    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    try
    {
      permsService.revokePermission(rUser.getOboTenantId(), username, systemId, path);
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_PERM_ERR", rUser, systemId, opName, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
    String msg = ApiUtils.getMsgAuth("FAPI_PERM_REVOKED", rUser, username, systemId, path);
    TapisResponse<String> response = TapisResponse.createSuccessResponse(msg, null);
    return Response.ok(response).build();
  }

  @GET
  @Path("/{systemId}/{path:(.*+)}")
  @Produces({ MediaType.APPLICATION_JSON })
  public TapisResponse<FilePermission> getPermissions(@PathParam("systemId") String systemId,
                                                      @PathParam("path") String path,
                                                      @QueryParam("username") String queryUsername,
                                                      @Context SecurityContext securityContext) throws NotFoundException
  {
    path = StringUtils.isBlank(path) ? "/" : path;
    String opName = "getPermissions";
    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    TapisSystem system;
    try
    {
      system = systemsCache.getSystem(oboTenant, systemId, oboUser);
      LibUtils.checkEnabled(rUser, system);
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SYSOPS_ERR", rUser, systemId, "getSystem", ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }

   String username = StringUtils.isBlank(queryUsername) ? oboUser : queryUsername;
    try
    {
      Permission permission = permsService.getPermission(oboTenant, username, systemId, path);
      FilePermission filePermission = new FilePermission();
      filePermission.setPath(path);
      filePermission.setSystemId(systemId);
      filePermission.setUsername(username);
      filePermission.setTenantId(oboTenant);
      filePermission.setPermission(permission);
      String msg = ApiUtils.getMsgAuth("FAPI_PERM_FOUND", rUser, filePermission, username, systemId, path);
      TapisResponse<FilePermission> response = TapisResponse.createSuccessResponse(msg, filePermission);
      return response;
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_PERM_ERR", rUser, systemId, opName, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
  }

  @POST
  @FilePermissionsAuthorization
  @Path("/{systemId}/{path:(.*+)}")
  @Produces({ MediaType.APPLICATION_JSON })
  @Consumes({MediaType.APPLICATION_JSON})
  public TapisResponse<FilePermission> grantPermissions(@PathParam("systemId") String systemId,
                                                        @PathParam("path") String path,
                                                        @Valid CreatePermissionRequest createPermissionRequest,
                                                        @Context SecurityContext securityContext) throws NotFoundException
  {
    path = StringUtils.isBlank(path) ? "/" : path;
    String opName = "grantPermissions";
    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    Permission perm = createPermissionRequest.getPermission();
    String user = createPermissionRequest.getUsername();
    String tenant = rUser.getOboTenantId();
    try
    {
      permsService.grantPermission(tenant, user, systemId, path, perm);
      FilePermission filePermission = new FilePermission();
      filePermission.setPath(path);
      filePermission.setSystemId(systemId);
      filePermission.setUsername(user);
      filePermission.setTenantId(tenant);
      filePermission.setPermission(perm);
      String msg = ApiUtils.getMsgAuth("FAPI_PERM_GRANTED", rUser, perm, user, systemId, path);
      TapisResponse<FilePermission> response = TapisResponse.createSuccessResponse(msg, filePermission);
      return response;
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_PERM_ERR", rUser, systemId, opName, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }
  }
}
