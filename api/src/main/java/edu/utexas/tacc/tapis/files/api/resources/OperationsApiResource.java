package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.lib.clients.FakeSystem;
import edu.utexas.tacc.tapis.files.lib.clients.FakeSystemsService;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
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

@Path("/ops")
public class OperationsApiResource {

    private final String EXAMPLE_SYSTEM_ID = "system123";
    private final String EXAMPLE_PATH = "/folderA/folderB/";
    private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();

    @Inject
    private FakeSystemsService systemsService;

    private Logger log = LoggerFactory.getLogger(OperationsApiResource.class);


    @GET
    @Path("/{systemId}")
    @Produces({ "application/json" })
    @Operation(summary = "List files/objects in a storage system.", description = "List files in a bucket", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "A list of files",
                    content = @Content(array = @ArraySchema(schema = @Schema(implementation = FileInfo.class)))
            )
    })
    public Response listFiles(
            @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
            @Parameter(description = "path relative to root of bucket/folder", example = EXAMPLE_PATH) @QueryParam("path") String path,
            @Parameter(description = "Return metadata also? This will slow down the request.") @QueryParam("meta") Boolean meta,
            @Context SecurityContext securityContext) throws NotFoundException {
        try {
            // First do SK check on system/path or throw 403

            // Fetch the system
            FakeSystem system = systemsService.getSystemByID(systemId);

            // Fetch the creds

            // build the client
            IRemoteDataClient client = clientFactory.getRemoteDataClient(system);
            List<FileInfo> listing = client.ls(path);
            return Response.ok(listing).build();
        } catch (IOException e) {
            log.error("Failed to list files", e);
            return Response.status(400).build();
        }
    }

    @POST
    @Path("/{systemId}/{path}")
    @Consumes({ "multipart/form-data" })
    @Operation(summary = "Upload a file", description = "The file will be added at the {path} independent of the original file name", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "OK")
    })
    public Response uploadFile(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "Path",required=true) @PathParam("path") String path,
            @FormDataParam("fileName") InputStream fileNameInputStream,
            @FormDataParam("fileName") FormDataContentDisposition fileNameDetail,
            @Parameter(description = "String dump of a valid JSON object to be associated with the file" ) @HeaderParam("x-meta") String xMeta,
            @Context SecurityContext securityContext) throws NotFoundException {
        return Response.ok().build();
    }

    @PUT
    @Path("/{systemId}/{path}")
    @Operation(summary = "Rename a file or folder", description = "Move/Rename a file in {systemID} at path {path}.", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "OK")
    })
    public Response rename(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true) @PathParam("path") String path,
            @Parameter(description = "",required=true) @QueryParam("newName") String newName,
            @Context SecurityContext securityContext)
            throws NotFoundException {
        return Response.ok().build();
    }


}
