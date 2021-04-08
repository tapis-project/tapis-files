package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.CreatePermissionRequest;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthorization;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.models.FilePermission;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.ResultSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
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
import java.util.Optional;


@Path("/v3/files/permissions")
public class PermissionsApiResource  {

    private static class FilePermissionResponse extends TapisResponse<FilePermission> {}
    private static class StringResponse extends TapisResponse<String> {}
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
    @Path("/{systemId}/{path:(.*+)}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Revoke user permission on a file or folder. ",
            description = "Revoke access to a file or folder for a user.",
            tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "FilePermission",
                    content = @Content(schema = @Schema(implementation = StringResponse.class))) }
    )
    public Response permissionsSystemIdPathDelete(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @NotEmpty @Parameter(description = "Username to remove",required=true) @QueryParam("username") String username,
            @Context SecurityContext securityContext) throws NotFoundException {
        String opName = "revokePermissions";
        path = StringUtils.isBlank(path) ? "/" : path;
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            permsService.revokePermission(user.getOboTenantId(), username, systemId, path);
            TapisResponse<String> response = TapisResponse.createSuccessResponse("Permission revoked.");
            return Response.ok(response).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_PERM_ERR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }


    @GET
    @Path("/{systemId}/{path:(.*+)}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(summary = "Get the API user's permission on a file or folder.",
            description = "Get the permission for the API user for the system and path.",
            tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "FilePermission",
                    content = @Content(schema = @Schema(implementation=FilePermissionResponse.class))) })
    public TapisResponse<FilePermission> getPermissions(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @Parameter(description = "Username to list") @QueryParam("username") String queryUsername,
            @Context SecurityContext securityContext) throws NotFoundException {

        path = StringUtils.isBlank(path) ? "/" : path;
        String opName = "getPermissions";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        ResultSystem system;
        String username;
        try {
            system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_SYSOPS_ERR", user, systemId, "getSystem", ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
        if (system == null) {
            throw new NotFoundException(Utils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));
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
            String msg = Utils.getMsgAuth("FILES_PERM_ERR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }

    @POST
    @FilePermissionsAuthorization
    @Path("/{systemId}/{path:(.*+)}")
    @Produces({ MediaType.APPLICATION_JSON })
    @Consumes({MediaType.APPLICATION_JSON})
    @Operation(summary = "Grant user permission on a file or folder. ",
            description = "Grant access to a file or folder for a user. Access may be READ or MODIFY. MODIFY implies READ.",
            tags={ "permissions" })
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
        path = StringUtils.isBlank(path) ? "/" : path;
        String opName = "grantPermissions";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            permsService.grantPermission(user.getOboTenantId(), createPermissionRequest.getUsername(), systemId, path, createPermissionRequest.getPermission());
            TapisResponse<String> response = TapisResponse.createSuccessResponse("Permissions granted.");
            return Response.ok(response).build();
        } catch (ServiceException ex) {
            String msg = Utils.getMsgAuth("FILES_PERM_ERR", user, systemId, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }
}