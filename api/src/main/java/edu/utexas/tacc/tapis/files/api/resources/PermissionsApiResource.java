package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.CreatePermissionRequest;
import edu.utexas.tacc.tapis.files.api.models.TransferTaskRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthorization;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.models.FilePermission;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;


@Path("/v3/files/permissions")
public class PermissionsApiResource  {

    private static class FilePermissionResponse extends TapisResponse<FilePermission> {}
    private static class FilePermissionStringResponse extends TapisResponse<String> {}
    private static final Logger log = LoggerFactory.getLogger(PermissionsApiResource.class);
    private final FilePermsService permsService;
    private final SystemsCache systemsCache;

    @Inject
    public PermissionsApiResource(FilePermsService permsService, SystemsCache systemsCache) {
        this.permsService = permsService;
        this.systemsCache = systemsCache;
    }

    @DELETE
    @FilePermissionsAuthorization
    @Path("/{systemId}/{path}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Remove permissions on an object for a user. ", description = "Remove user permissions to a file/folder.", tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "FilePermission",
                    content = @Content(schema = @Schema(implementation = FilePermissionStringResponse.class))) }
    )
    public Response permissionsSystemIdPathDelete(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @NotEmpty @Parameter(description = "Username to remove",required=true) @QueryParam("username") String username,
            @Context SecurityContext securityContext) throws NotFoundException {
        String opName = "revokePermissions";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            permsService.revokePermission(user.getOboTenantId(), username, systemId, path, FilePermissionsEnum.ALL);
            TapisResponse<String> response = TapisResponse.createSuccessResponse("Permissions revoked.");
            return Response.ok(response).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILESAPI_PERM_ERROR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }


    @GET
    @Path("/{systemId}/{path}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Get the API user's permissions on a file or folder.", description = "Get the permissions for the API user for the system and path.", tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "FilePermission",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = FilePermissionResponse.class)))) })
    public Response permissionsSystemIdPathGet(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @Parameter(description = "Username to list") @QueryParam("username") String queryUsername,
            @Context SecurityContext securityContext) throws NotFoundException {

        String opName = "getPermissions";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        TSystem system;
        String username;
        try {
            system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILESAPI_SYSOPS_ERROR", user, systemId, "getSystem", ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
        if (system == null) {
            throw new NotFoundException(Utils.getMsgAuth("FILESAPI_SYS_NOTFOUND", user, systemId));
        }

        if (queryUsername == null) {
            username = user.getOboUser();
        } else {
            username = queryUsername;
        }

        try {
            boolean allPermitted = permsService.isPermitted(user.getOboTenantId(), username, systemId, path, FilePermissionsEnum.ALL);
            boolean readPermitted = permsService.isPermitted(user.getOboTenantId(), username, systemId, path, FilePermissionsEnum.READ);
            FilePermission permission = new FilePermission();
            permission.setPath(path);
            permission.setSystemId(systemId);
            permission.setUsername(username);
            permission.setTenantId(user.getOboTenantId());
            if (readPermitted)
                permission.setPermissions(FilePermissionsEnum.READ);
            if (allPermitted)
                permission.setPermissions(FilePermissionsEnum.ALL);

            TapisResponse<FilePermission> response = TapisResponse.createSuccessResponse(permission);
            return Response.ok(response).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILESAPI_PERM_ERROR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }

    @POST
    @FilePermissionsAuthorization
    @Path("/{systemId}/{path}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes({MediaType.APPLICATION_JSON})
    @Operation(summary = "Add permissions on an object. ", description = "Add a user to a file/folder.", tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "FilePermission",
                    content = @Content(schema = @Schema(implementation = FilePermissionResponse.class)))
    })
    public Response permissionsSystemIdPathPost(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @Valid @Parameter(required = true) CreatePermissionRequest createPermissionRequest,
            @Context SecurityContext securityContext) throws NotFoundException {

        String opName = "grantPermissions";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            permsService.grantPermission(user.getOboTenantId(), createPermissionRequest.getUsername(), systemId, path, createPermissionRequest.getPermission());
            TapisResponse<String> response = TapisResponse.createSuccessResponse("Permissions granted.");
            return Response.ok(response).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILESAPI_PERM_ERROR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

    }
}