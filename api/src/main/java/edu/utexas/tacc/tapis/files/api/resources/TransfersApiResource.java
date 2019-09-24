package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;


@Path("/transfers")
public class TransfersApiResource {

  private final String EXAMPLE_SYSTEM_ID = "system123";
  private final String EXMAPLE_PATH = "/folderA/folderB/";

  @GET
  @Path("/{transferTaskId}/")
  @Operation(summary = "Get the status of a transfer task", description = "", tags={ "transfers" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "OK")
  })
  public Response getTransferTaskStatus(
      @Parameter(description = "Transfer task ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("transferTaskId") String systemId,
      @Parameter(description = "Range of bytes to send" ) @HeaderParam("Range") String range,
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }


  @DELETE
  @Path("/{transferTaskId}")
  @Produces({ "application/json" })
  @Operation(summary = "Stop/Cancel a transfer task", description = "", tags={ "transfers" })
  @ApiResponses(value = {
      @ApiResponse(
          responseCode = "200",
          description = "OK"
      )
  })
  public Response cancelTransferTask(
      @Parameter(description = "Transfer task ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("transferTaskId") String systemId,
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }

  @POST
  @Produces({ "application/json" })
  @Operation(
      summary = "Transfer data",
      description = "This creates a background task which will transfer files into the storage system",
      tags={ "transfers" }
  )
  @ApiResponses(value = {
      @ApiResponse(
          responseCode = "200",
          description = "OK",
          content = @Content(schema = @Schema(implementation = TransferTask.class))
      )
  })
  public Response createTransferTask(
      @Context SecurityContext securityContext) throws NotFoundException {
    return Response.ok().build();
  }
}
