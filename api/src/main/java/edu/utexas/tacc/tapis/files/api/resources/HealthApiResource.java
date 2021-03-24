package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicLong;

@Path("/v3/files/healthcheck")
public class HealthApiResource {

  // Count the number of health check requests received.
  private static final AtomicLong healthCheckCount = new AtomicLong();

    private static class HealthCheckResponse extends TapisResponse<String>{}

    @GET
    @PermitAll
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Health check", description = "Health check", tags={ "health" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = HealthCheckResponse.class))
            )
    })
    public Response healthCheck() throws NotFoundException {
      // Get the current check count.
      long checkNum = healthCheckCount.incrementAndGet();
      TapisResponse resp = TapisResponse.createSuccessResponse("Health check received. Count: " + checkNum);
      return Response.ok(resp).build();
    }

}
