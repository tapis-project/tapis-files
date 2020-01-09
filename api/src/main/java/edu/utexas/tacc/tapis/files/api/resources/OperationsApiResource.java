package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.models.CreateDirectoryRequest;
import edu.utexas.tacc.tapis.files.api.utils.TapisResponse;
import edu.utexas.tacc.tapis.files.lib.clients.FakeSystemsService;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

@Path("/ops")
public class OperationsApiResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();

    @Inject
    private FakeSystemsService systemsService;

    private Logger log = LoggerFactory.getLogger(OperationsApiResource.class);
    private static class FileListingResponse extends TapisResponse<List<FileInfo>>{}
    private static class FileStringResponse extends TapisResponse<String>{}


    @GET
    @Path("/{systemId}/{path:.+}")
    @Produces(MediaType.APPLICATION_JSON)
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
            @Context SecurityContext securityContext)  {
        try {

            // First do SK check on system/path or throw 403
            // TODO Waiting on Security Kernel to implement isPermitted for Files

            // Fetch the system based on the systemId
            TSystem sys = systemsService.getSystemByName(systemId);

            // Fetch the creds
            //TODO creds in system service and in SK are being implemented. After that it will be implemented here in files

            IRemoteDataClient client = clientFactory.getRemoteDataClient(sys);
            client.connect();

            List<FileInfo> listing = client.ls(path);
            client.disconnect();
            TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse("ok",listing);
            return Response.status(Status.OK).entity(resp).build();

        } catch (IOException e) {
            log.error("listFiles", e);
            throw new WebApplicationException("server error");
        }
    }

    @POST
    @Path("/{systemId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Create a new directory", description = "Creates a new directory in the path given in the payload", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
                    description = "OK")
    })
    public Response mkdir(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Valid @Parameter(required = true) CreateDirectoryRequest mkdirRequest,
            @Context SecurityContext securityContext) {
        try {
            //TODO: Permissions
            TSystem sys = systemsService.getSystemByName(systemId);
            // Fetch the creds
            //TODO creds in system service and in SK are being implemented. After that it will be implemented here in files
            IRemoteDataClient client = clientFactory.getRemoteDataClient(sys);
            client.mkdir(mkdirRequest.getPath());


            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (IOException ex) {
            log.error(ex.getMessage());
            throw new WebApplicationException();
        }
    }




    @POST
    @Path("/{systemId}/{path:.+}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Operation(summary = "Upload a file", description = "The file will be added at the {path} independent of the original file name", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
                    description = "OK")
    })
    public Response insert(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "Path",required=true) @PathParam("path") String path,
            @FormDataParam("file") InputStream fileInputStream,
            @FormDataParam("file") FormDataContentDisposition fileNameDetail,
            @Parameter(description = "String dump of a valid JSON object to be associated with the file" ) @HeaderParam("x-meta") String xMeta,
            @Context SecurityContext securityContext) {
        try {
            //TODO: Permissions
            TSystem sys = systemsService.getSystemByName(systemId);
            // Fetch the creds
            //TODO creds in system service and in SK are being implemented. After that it will be implemented here in files
            IRemoteDataClient client = clientFactory.getRemoteDataClient(sys);

            //If file in form is null then mkdir at the path given
            if (fileInputStream == null) {
                client.mkdir(path);
            } else {
                client.connect();
                client.insert(path, fileInputStream);
            }


            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (IOException ex) {
            log.error(ex.getMessage());
            throw new WebApplicationException();
        }
    }


    @PUT
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Rename a file or folder", description = "Move/Rename a file in {systemID} at path {path}.", tags={ "file operations" })
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class))
            )
    })
    public Response rename(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true) @PathParam("path") String path,
            @Parameter(description = "The new name of the file/folder",required=true) @QueryParam("newName") String newName,
            @Context SecurityContext securityContext) {

        try {
            // Fetch the system based on the systemId
            TSystem sys = systemsService.getSystemByName(systemId);

            // Fetch the creds
            //TODO creds in system service and in SK are being implemented. After that it will be implemented here in files

            IRemoteDataClient client = clientFactory.getRemoteDataClient(sys);
            client.connect();

            client.move(path, newName);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (IOException ex) {
            log.error("rename", ex);
            throw new WebApplicationException();
        }


    }

    @DELETE
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Delete a file or folder", description = "Delete a file in {systemID} at path {path}.", tags={ "file operations" })
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "OK",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class))
            )
    })
    public Response delete(
            @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=false) @PathParam("path") String path,
            @Context SecurityContext securityContext) {
        // Fetch the system based on the systemId
        try {
            TSystem sys = systemsService.getSystemByName(systemId);

            // Fetch the creds
            //TODO creds in system service and in SK are being implemented. After that it will be implemented here in files

            IRemoteDataClient client = clientFactory.getRemoteDataClient(sys);
            client.connect();

            client.delete(path);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (IOException ex) {
            log.error("rename", ex);
            throw new WebApplicationException();
        }

    }

}
