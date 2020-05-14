package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.dao.permissions.FilePermission;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;
import javax.validation.constraints.*;


@Path("/v3/files/permissions")
public class PermissionsApiResource  {

    private static class FilePermissionResponse extends TapisResponse<FilePermission> {}


    @DELETE
    @Path("/{systemId}/{path}")
    @Produces({ "application/json" })
    @Operation(summary = "Remove permissions on an object for a user. ", description = "Remove user permissions to a file/folder. QUESTION - who should be able to delete permissions? Only the owner? ", tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "FilePermission",
                    content = @Content(schema = @Schema(implementation = FilePermissionResponse.class))) }
    )
    public Response permissionsSystemIdPathDelete(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @Parameter(description = "Username to remove",required=true) @QueryParam("username") String username,
            @Context SecurityContext securityContext) throws NotFoundException {
        return Response.ok().build();
    }


    @GET
    @Path("/{systemId}/{path}")
    @Produces({ "application/json" })
    @Operation(summary = "List permissions on an file/folder", description = "Returns a list of roles/users that can access the file QUESTION: Who should be able to see access this? Only the owner of the system? ", tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "FilePermission",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = FilePermissionResponse.class)))) })
    public Response permissionsSystemIdPathGet(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @Context SecurityContext securityContext) throws NotFoundException {
        return Response.ok().build();
    }

    @POST
    @Path("/{systemId}/{path}")
    @Produces({ "application/json" })
    @Operation(summary = "Add permissions on an object. ", description = "Add a user to a file/folder. QUESTION - who should be able to add permissions? Only the owner? ", tags={ "permissions" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "FilePermission",
                    content = @Content(schema = @Schema(implementation = FilePermissionResponse.class)))
    })
    public Response permissionsSystemIdPathPost(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "path",required=true) @PathParam("path") String path,
            @Context SecurityContext securityContext) throws NotFoundException {
        return Response.ok().build();
    }
}