package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

@Path("/v3/files/health")
public class HealthApiResource {

    private static class HealthCheckResponse extends TapisResponse<String>{}

    @GET
    @PermitAll
    @Path("/healthcheck")
    @Operation(summary = "Health check", description = "Health check", tags={ "health" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = HealthCheckResponse.class))
            )
    })
    public Response healthCheck() throws NotFoundException {
        TapisResponse resp = TapisResponse.createSuccessResponse("ok");
        return Response.ok(resp).build();
    }

}
