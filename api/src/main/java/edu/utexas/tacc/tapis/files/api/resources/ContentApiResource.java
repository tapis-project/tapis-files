package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.clients.*;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Path("/content")
public class ContentApiResource {

  private final String EXAMPLE_SYSTEM_ID = "system123";
  private final String EXAMPLE_PATH = "/folderA/folderB/";
  private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();

  @Inject
  private FakeSystemsService systemsService;

  private Logger log = LoggerFactory.getLogger(ContentApiResource.class);

  @GET
  @Path("/{systemId}/{path}")
  @Operation(summary = "Retrieve a file from the files service", description = "Get file contents/serve file", tags={ "content" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "OK")
  })
  public Response filesGetContents(
      @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
      @Parameter(description = "File path",required=true, example = EXAMPLE_PATH) @PathParam("path") String path,
      @Parameter(description = "Range of bytes to send" ) @HeaderParam("Range") String range,
      @Context SecurityContext securityContext) throws NotFoundException {

    // First do SK check on system/path

    // Fetch the system

    // Fetch the creds

    // build the client


    return Response.ok().build();
  }

}
