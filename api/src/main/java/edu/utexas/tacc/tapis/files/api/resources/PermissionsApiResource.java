package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.CreatePermissionRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthorization;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.models.FilePermission;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

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
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            permsService.revokePermission(user.getOboTenantId(), username, systemId, path);
            TapisResponse<String> response = TapisResponse.createSuccessResponse("Permission revoked.");
            return Response.ok(response).build();
        } catch (ServiceException ex) {
            String msg = LibUtils.getMsgAuth("FILES_PERM_ERR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
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
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        TapisSystem system;
        String username;
        try {
            system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            LibUtils.checkEnabled(user, system);
        } catch (ServiceException ex) {
            String msg = LibUtils.getMsgAuth("FILES_SYSOPS_ERR", user, systemId, "getSystem", ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
        if (system == null) {
            throw new NotFoundException(LibUtils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));
        }

        if (queryUsername == null) {
            username = user.getOboUser();
        } else {
            username = queryUsername;
        }

        try {
            Permission permission = permsService.getPermission(user.getOboTenantId(), username, systemId, path);
            FilePermission filePermission = new FilePermission();
            filePermission.setPath(path);
            filePermission.setSystemId(systemId);
            filePermission.setUsername(username);
            filePermission.setTenantId(user.getOboTenantId());
            filePermission.setPermission(permission);
            TapisResponse<FilePermission> response = TapisResponse.createSuccessResponse(filePermission);
            return response;
        } catch (ServiceException ex) {
            String msg = LibUtils.getMsgAuth("FILES_PERM_ERR", user, systemId, opName, ex.getMessage());
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
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            permsService.grantPermission(user.getOboTenantId(), createPermissionRequest.getUsername(), systemId, path, createPermissionRequest.getPermission());
            FilePermission filePermission = new FilePermission();
            filePermission.setPath(path);
            filePermission.setSystemId(systemId);
            filePermission.setUsername(createPermissionRequest.getUsername());
            filePermission.setTenantId(user.getOboTenantId());
            filePermission.setPermission(createPermissionRequest.getPermission());
            TapisResponse<FilePermission> response = TapisResponse.createSuccessResponse(filePermission);
            return response;
        } catch (ServiceException ex) {
            String msg = LibUtils.getMsgAuth("FILES_PERM_ERR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }
}
