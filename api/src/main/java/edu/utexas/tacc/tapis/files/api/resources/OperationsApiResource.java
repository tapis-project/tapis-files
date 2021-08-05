package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyOperation;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRequest;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Path("/v3/files/ops")
public class OperationsApiResource extends BaseFileOpsResource {

    private static final int MAX_RECURSION_DEPTH = 10;
    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private static final Logger log = LoggerFactory.getLogger(OperationsApiResource.class);

    private static class FileListingResponse extends TapisResponse<List<FileInfo>> {
    }

    private static class FileStringResponse extends TapisResponse<String> {
    }

    @Inject
    IFileOpsService fileOpsService;


    @GET
    @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List files/objects in a storage system.", description = "List files in a storage system", tags = {"file operations"})
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileListingResponse.class)),
            description = "A list of files"),
        @ApiResponse(
            responseCode = "400",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Bad Request"),
        @ApiResponse(
            responseCode = "401",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authenticated"),
        @ApiResponse(
            responseCode = "403",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authorized"),
        @ApiResponse(
            responseCode = "404",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Found"),
        @ApiResponse(
            responseCode = "500",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Internal Error")
    })
    public Response listFiles(
        @Parameter(description = "System ID", required = true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
        @Parameter(description = "path relative to root of bucket/folder", required = false, example = EXAMPLE_PATH) @PathParam("path") String path,
        @Parameter(description = "pagination limit", example = "100") @DefaultValue("1000") @QueryParam("limit") @Max(1000) int limit,
        @Parameter(description = "pagination offset", example = "1000") @DefaultValue("0") @QueryParam("offset") @Min(0) long offset,
        @Parameter(description = "recursive listing", example = "false") @DefaultValue("false") @QueryParam("recurse") boolean recurse,
        @Context SecurityContext securityContext) {
        String opName = "listFiles";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();

        try {
            Instant start = Instant.now();
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            List<FileInfo> listing = new ArrayList<>();
            if (recurse) {
                listing = fileOpsService.lsRecursive(client, path, MAX_RECURSION_DEPTH);
            } else {
                listing = fileOpsService.ls(client, path, limit, offset);
            }
            String msg = Utils.getMsgAuth("FILES_DURATION", user, opName, systemId, Duration.between(start, Instant.now()).toMillis());
            log.debug(msg);
            TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse("ok", listing);
            return Response.status(Status.OK).entity(resp).build();
        } catch (NotFoundException e) {
            throw new NotFoundException(Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage()));
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

    @POST
    @Path("/{systemId}/{path:.+}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Upload a file",
        description = "The file will be added at the {path} independent of the original file name",
        tags = {"file operations"})
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "OK"),
        @ApiResponse(
            responseCode = "400",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Bad Request"),
        @ApiResponse(
            responseCode = "401",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authenticated"),
        @ApiResponse(
            responseCode = "403",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authorized"),
        @ApiResponse(
            responseCode = "500",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Internal Error")
    })
    public Response insert(
        @Parameter(description = "System ID", required = true) @PathParam("systemId") String systemId,
        @Parameter(description = "Path", required = true) @PathParam("path") String path,
        @Parameter(required = true, schema = @Schema(type = "string", format = "binary")) @FormDataParam(value = "file") InputStream fileInputStream,
        @Context SecurityContext securityContext) {
        String opName = "insert";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            fileOpsService.insert(client, path, fileInputStream);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }


    @POST
    @Path("/{systemId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Create a directory", description = "Create a directory in the system at path the given path", tags = {"file operations"})
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "OK"),
        @ApiResponse(
            responseCode = "400",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Bad Request"),
        @ApiResponse(
            responseCode = "401",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authenticated"),
        @ApiResponse(
            responseCode = "403",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authorized"),
        @ApiResponse(
            responseCode = "500",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Internal Error")
    })
    public Response mkdir(
        @Parameter(description = "System ID", required = true) @PathParam("systemId") String systemId,
        @Valid @Parameter MkdirRequest mkdirRequest,
        @Context SecurityContext securityContext) {
        String opName = "mkdir";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, null);
            fileOpsService.mkdir(client, mkdirRequest.getPath());
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, mkdirRequest.getPath(), e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

    @PUT
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Move/copy a file or folder", description = "Move/copy a file in {systemID} at path {path}.", tags = {"file operations"})
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "OK"),
        @ApiResponse(
            responseCode = "400",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Bad Request"),
        @ApiResponse(
            responseCode = "401",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authenticated"),
        @ApiResponse(
            responseCode = "403",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authorized"),
        @ApiResponse(
            responseCode = "404",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Found"),
        @ApiResponse(
            responseCode = "500",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Internal Error")
    })
    public Response moveCopy(
        @Parameter(description = "System ID", required = true) @PathParam("systemId") String systemId,
        @Parameter(description = "File path", required = true) @PathParam("path") String path,
        @Valid MoveCopyRequest request,
        @Context SecurityContext securityContext) {
        String opName = "moveCopy";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            if (client == null) {
                throw new NotFoundException(Utils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));
            }
            MoveCopyOperation operation = request.getOperation();
            if (operation.equals(MoveCopyOperation.MOVE)) {
                fileOpsService.move(client, path, request.getNewPath());
            } else if (operation.equals(MoveCopyOperation.COPY)) {
                fileOpsService.copy(client, path, request.getNewPath());
            }
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

    @DELETE
    @Path("/{systemId}/{path:(.*+)}")
    @Operation(summary = "Delete a file or folder", description = "Delete a file in {systemID} at path {path}.", tags = {"file operations"})
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "OK"),
        @ApiResponse(
            responseCode = "400",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Bad Request"),
        @ApiResponse(
            responseCode = "401",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authenticated"),
        @ApiResponse(
            responseCode = "403",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Authorized"),
        @ApiResponse(
            responseCode = "404",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Not Found"),
        @ApiResponse(
            responseCode = "500",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "Internal Error")

    })
    public Response delete(
        @Parameter(description = "System ID", required = true) @PathParam("systemId") String systemId,
        @Parameter(description = "File path", required = false) @PathParam("path") String path,
        @Context SecurityContext securityContext) {
        String opName = "delete";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            fileOpsService.delete(client, path);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }
}
