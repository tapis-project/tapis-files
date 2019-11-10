package edu.utexas.tacc.tapis.files.api.resources;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

@Path("/health")
public class HealthApiResource {
  @GET
  @PermitAll
  @Path("/healthcheck}")
  @Operation(summary = "Health check", description = "Health check", tags={ "health" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "OK")
  })
  public Response healthCheck() throws NotFoundException {
    return Response.ok().build();
  }

}
