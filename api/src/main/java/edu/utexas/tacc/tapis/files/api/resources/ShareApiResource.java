package edu.utexas.tacc.tapis.files.api.resources;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import edu.utexas.tacc.tapis.files.lib.models.SharedFileObject;
import edu.utexas.tacc.tapis.files.api.models.ShareFileRequest;

@Path("/v3/files/share")
public class ShareApiResource  {

  @DELETE
  @Path("/{systemId}/{path}")

  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Revoke a shared file resource ", description = "Removes any outstanding shares on a file resource. ", tags={ "share" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "Shared file object",
          content = @Content(schema = @Schema(implementation = SharedFileObject.class)))
  })
  public Response shareDelete(
      @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
      @Parameter(description = "System ID",required=true) @PathParam("path") String path,
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }


  @GET
  @Path("/{systemId}/{path}")

  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List the shares on a file resource. ", description = "List all shares on a given file resource. ", tags={ "share" })
  @ApiResponses(value = {
      @ApiResponse(
          responseCode = "200",
          description = "List of shares",
          content = @Content(array = @ArraySchema(schema = @Schema(implementation = SharedFileObject.class))))
  })
  public Response shareList (
      @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
      @Parameter(description = "System ID",required=true) @PathParam("path") String path,
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }


  @POST
  @Path("/{systemId}/{path}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Grant temporary access to a file resource. ",
      description = "Creates a link that is valid for the requested validity time for the given user for the resource in {systemId} at path {path} ",
      tags={ "share" })
  @ApiResponses(value = {
      @ApiResponse(
          responseCode = "200",
          description = "Shared file object",
          content = @Content(schema = @Schema(implementation = SharedFileObject.class)))
  })
  public Response shareFile (
      @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
      @Parameter(description = "path",required=true) @PathParam("path") String path,
      @Parameter(description = "" ) ShareFileRequest body,
      @Context SecurityContext securityContext) throws NotFoundException {
    // Add row to security kernel?
    return Response.ok().build();
  }
}