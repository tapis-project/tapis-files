package edu.utexas.tacc.tapis.files.api.resources;


import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.api.responses.RespFilesList;
import edu.utexas.tacc.tapis.files.api.responses.results.ResultFilesList;

import edu.utexas.tacc.tapis.files.api.utils.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.clients.FakeSystem;

import edu.utexas.tacc.tapis.files.lib.clients.FakeSystemsService;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.sharedapi.utils.RestUtils;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

@Path("/ops")
public class OperationsApiResource {

    private final String EXAMPLE_SYSTEM_ID = "system123";
    private final String EXAMPLE_PATH = "/folderA/folderB/";
    private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();

    @Inject
    private FakeSystemsService systemsService;

    private Logger log = LoggerFactory.getLogger(OperationsApiResource.class);

    private static class FileListingResponse extends TapisResponse<List<FileInfo>>{}
    private static class FileStringResponse extends TapisResponse<String>{}


    @GET
    @Path("/{systemId}/{path}")
    @Produces({ "application/json" })
    @Operation(summary = "List files/objects in a storage system.", description = "List files in a bucket", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "A list of files",
                    content = @Content(schema = @Schema(implementation = FileListingResponse.class))
            )
    })
    public Response listFiles(
            @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
            @Parameter(description = "path relative to root of bucket/folder", example = EXAMPLE_PATH) @PathParam("path") String path,
            @Parameter(description = "pagination limit", example = "100") @QueryParam("limit") int limit,
            @Parameter(description = "pagination offset", example = "1000") @QueryParam("offset") int offset,
            @Parameter(description = "Return metadata also? This will slow down the request.") @QueryParam("meta") Boolean meta,
            @Context SecurityContext securityContext) throws NotFoundException {
        try {

            // First do SK check on system/path or throw 403
            // TODO Waiting on Security Kernel to implement isPermitted for Files


            // Fetch the system based on the systemId
            // TODO System Service is being implemented. We are able to used some of fields of Systems that System service have already implemented.
            System.out.println("Get System based on System ID:" + systemId);

            //FakeSystem system = systemsService.getSystemByID(systemId);
            TSystem sys = systemsService.getSystemByName(systemId);

            // Fetch the creds
            //TODO creds in system service and in SK are being implemented. After that it will be implemented here in files

            // build the client
            log.debug("Get Remote Data Client");

            IRemoteDataClient client = clientFactory.getRemoteDataClient(sys);
            client.connect();

            log.debug("Do the files lisiting for a specific path: " + path);
            List<FileInfo> listing = client.ls(path);
            System.out.println("listing in API:" + listing);

            client.disconnect();

            TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse("ok",listing);
            return Response.status(Status.OK).entity(resp).build();

        } catch (IOException e) {
            throw new WebApplicationException("server error");
        }
    }


    @POST
    @Path("/{systemId}/{path}")
    @Consumes({ "multipart/form-data" })
    @Operation(summary = "Upload a file", description = "The file will be added at the {path} independent of the original file name", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
                    description = "OK")
    })
    public Response uploadFile(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "Path",required=true) @PathParam("path") String path,
            @FormDataParam("fileName") InputStream fileNameInputStream,
            @FormDataParam("fileName") FormDataContentDisposition fileNameDetail,
            @Parameter(description = "String dump of a valid JSON object to be associated with the file" ) @HeaderParam("x-meta") String xMeta,
            @Context SecurityContext securityContext) throws NotFoundException {

        TapisResponse resp = TapisResponse.createSuccessResponse("ok", "ok");
        return Response.ok(resp).build();
    }


    @PUT
    @Path("/{systemId}/{path}")
    @Operation(summary = "Rename a file or folder", description = "Move/Rename a file in {systemID} at path {path}.", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class))
            )
    })
    public Response rename(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true) @PathParam("path") String path,
            @Parameter(description = "",required=true) @QueryParam("newName") String newName,
            @Context SecurityContext securityContext)
            throws NotFoundException {
        TapisResponse resp = TapisResponse.createSuccessResponse("ok");
        return Response.ok(resp).build();
    }

    @DELETE
    @Path("/{systemId}/{path}")
    @Operation(summary = "Delete a file or folder", description = "Delete a file in {systemID} at path {path}.", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class))
            )
    })
    public Response delete(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true) @PathParam("path") String path,
            @Context SecurityContext securityContext)
            throws NotFoundException {
        TapisResponse resp = TapisResponse.createSuccessResponse("ok");
        return Response.ok(resp).build();
    }

}
