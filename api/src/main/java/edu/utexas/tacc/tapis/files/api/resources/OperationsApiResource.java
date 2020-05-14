package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthorization;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.*;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
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
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Path("/v3/files/ops")
public class OperationsApiResource extends BaseFilesResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private static final Logger log = LoggerFactory.getLogger(OperationsApiResource.class);
    private static class FileListingResponse extends TapisResponse<List<FileInfo>>{}
    private static class FileStringResponse extends TapisResponse<String>{}

    @Inject
    SystemsClient systemsClient;

    @Inject
    RemoteDataClientFactory remoteDataClientFactory;

    @GET
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.READ)
    @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
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
            @Parameter(description = "path relative to root of bucket/folder", required=false, example = EXAMPLE_PATH) @PathParam("path") String path,
            @Parameter(description = "pagination limit", example = "100") @DefaultValue("1000") @QueryParam("limit") @Max(1000) int limit,
            @Parameter(description = "pagination offset", example = "1000") @DefaultValue("0") @QueryParam("offset") @Min(0) long offset,
            @Parameter(description = "Return metadata also? This will slow down the request.") @QueryParam("meta") Boolean meta,
            @Context SecurityContext securityContext)  {
        try {
            AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
            configureSystemsClient(user);
            TSystem system = systemsClient.getSystemByName(systemId, null);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, user.getOboUser());
            FileOpsService fileOpsService = new FileOpsService(client);
            List<FileInfo> listing = fileOpsService.ls(path, limit, offset);
            TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse("ok",listing);
            return Response.status(Status.OK).entity(resp).build();
        } catch (ServiceException | IOException | TapisException e) {
            log.error("listFiles", e);
            throw new WebApplicationException(e.getMessage());
        }
    }


    @POST
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.ALL)
    @Path("/{systemId}/{path:.+}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Operation(summary = "Create a directory", description = "Create a directory in the system at path the given path", tags={ "file operations" })
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
                    description = "OK")
    })
    public Response mkdir(
            @Parameter(description = "System ID", required=true) @PathParam("systemId") String systemId,
            @Parameter(description = "Path", required=true) @Pattern(regexp = "^(?!.*\\.).+", message=". not allowed in path") @PathParam("path") String path,
            @Context SecurityContext securityContext) {
        try {
            AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
            configureSystemsClient(user);
            TSystem system = systemsClient.getSystemByName(systemId, null);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, user.getOboUser());
            FileOpsService fileOpsService = new FileOpsService(client);
            fileOpsService.mkdir(path);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException | TapisClientException ex) {
            log.error("mkdir", ex);
            throw new WebApplicationException("Something went wrong...");
        }
    }


    @POST
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.ALL)
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
            AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
            configureSystemsClient(user);
            TSystem system = systemsClient.getSystemByName(systemId, null);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, user.getOboUser());
            FileOpsService fileOpsService = new FileOpsService(client);            fileOpsService.insert(path, fileInputStream);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException | TapisClientException ex) {
            log.error(ex.getMessage());
            throw new WebApplicationException();
        }
    }


    @PUT
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.ALL)
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
            AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
            configureSystemsClient(user);
            TSystem system = systemsClient.getSystemByName(systemId, null);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, user.getOboUser());
            FileOpsService fileOpsService = new FileOpsService(client);            fileOpsService.move(path, newName);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException | TapisClientException ex) {
            log.error("rename", ex);
            throw new WebApplicationException();
        }


    }

    @DELETE
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.ALL)
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
        try {
            AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
            configureSystemsClient(user);
            TSystem system = systemsClient.getSystemByName(systemId, null);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, user.getOboUser());
            FileOpsService fileOpsService = new FileOpsService(client);            fileOpsService.delete(path);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException | TapisClientException ex) {
            log.error("delete", ex);
            throw new WebApplicationException(ex.getMessage());
        }
    }
    
}


