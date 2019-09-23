package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class SystemsApiResource {

  @GET
  @Path("/{systemId}/{path}")
  @Operation(summary = "Retrieve a file from the files service", description = "Get file contents/serve file", tags={ "file operations" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "OK") })
  public Response filesGetContents(
      @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
      @Parameter(description = "File path",required=true) @PathParam("path") String path,
      @Parameter(description = "Range of bytes to send" )@HeaderParam("Range") String range,
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }


  @GET
  @Path("/{systemId}")
  @Produces({ "application/json" })
  @Operation(summary = "List files/objects in a storage system.", description = "List files in a bucket", tags={ "file operations" })
  @ApiResponses(value = {
      @ApiResponse(
          responseCode = "200",
          description = "A list of files",
          content = @Content(array = @ArraySchema(schema = @Schema(implementation = FileInfo.class))))
  })
  public Response filesList(
      @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
      @Parameter(description = "path relative to root of bucket") @QueryParam("path") String path,
      @Parameter(description = "Return metadata also? This will slow down the request.") @QueryParam("meta") Boolean meta,
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }
}
