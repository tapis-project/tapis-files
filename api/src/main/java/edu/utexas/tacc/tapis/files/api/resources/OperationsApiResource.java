package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.models.MkdirRequest;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRenameOperation;
import edu.utexas.tacc.tapis.files.api.models.MoveCopyRenameRequest;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthorization;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
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
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Path("/v3/files/ops")
public class OperationsApiResource extends BaseFileOpsResource {

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
    @FileOpsAuthorization(permRequired = Permission.READ)
    @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "List files/objects in a storage system.", description = "List files in a bucket", tags = {"file operations"})
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileListingResponse.class)),
            description = "A list of files"),
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
        @Context SecurityContext securityContext) {
        String opName = "listFiles";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            Instant start = Instant.now();
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
            List<FileInfo> listing = fileOpsService.ls(client, path, limit, offset);
            //TODO: This feels like it should be in the service layer
            listing.forEach(f-> {
                String uri = String.format("tapis://%s/%s/%s", user.getOboTenantId(), systemId, f.getPath());
                f.setUri(uri);
            });
          String msg = Utils.getMsgAuth("FILES_DURATION", user, opName, systemId, Duration.between(start, Instant.now()).toMillis());
          log.debug(msg);
            TapisResponse<List<FileInfo>> resp = TapisResponse.createSuccessResponse("ok", listing);
            return Response.status(Status.OK).entity(resp).build();
        }
        catch (NotFoundException e) {
          throw new NotFoundException(Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage()));
        }
        catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

    @POST
    @FileOpsAuthorization(permRequired = Permission.MODIFY)
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
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
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
    @FileOpsAuthorization(permRequired = Permission.MODIFY)
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
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
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
    @FileOpsAuthorization(permRequired = Permission.MODIFY)
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Move/copy/rename a file or folder", description = "Move/Rename a file in {systemID} at path {path}.", tags = {"file operations"})
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "OK"),
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
    public Response moveCopyRename(
        @Parameter(description = "System ID", required = true) @PathParam("systemId") String systemId,
        @Parameter(description = "File path", required = true) @PathParam("path") String path,
        @Valid MoveCopyRenameRequest request,
        @Context SecurityContext securityContext) {
        String opName = "moveCopyRename";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
            if (client == null) {
                throw new NotFoundException(Utils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));
            }
            MoveCopyRenameOperation operation = request.getOperation();
            if (operation.equals(MoveCopyRenameOperation.MOVE)) {
                fileOpsService.move(client, path, request.getNewPath());
            } else if (operation.equals(MoveCopyRenameOperation.COPY)) {
                fileOpsService.copy(client, path, request.getNewPath());
            } else if (operation.equals(MoveCopyRenameOperation.RENAME)) {
                java.nio.file.Path tmp = Paths.get(path).resolveSibling(request.getNewPath());
                fileOpsService.move(client, path, tmp.toString());
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
    @FileOpsAuthorization(permRequired = Permission.MODIFY)
    @Path("/{systemId}/{path:(.*+)}")
    @Operation(summary = "Delete a file or folder", description = "Delete a file in {systemID} at path {path}.", tags = {"file operations"})
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStringResponse.class)),
            description = "OK"),
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
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
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
