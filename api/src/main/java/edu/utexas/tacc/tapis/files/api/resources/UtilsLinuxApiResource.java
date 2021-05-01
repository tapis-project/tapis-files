package edu.utexas.tacc.tapis.files.api.resources;


import edu.utexas.tacc.tapis.files.api.models.NativeLinuxOpRequest;
import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthorization;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.services.IFileUtilsService;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/*
 * JAX-RS REST api for utility methods.
 * TODO/TBD: Have systemType in top level path or make it a path parameter
 *           (and rename this class to UtilsApiResource)?
 */
@Path("/v3/files/utils/linux")
public class UtilsLinuxApiResource extends BaseFileOpsResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/file1";
    private static final Logger log = LoggerFactory.getLogger(UtilsLinuxApiResource.class);

    private static class FileStatInfoResponse extends TapisResponse<List<FileStatInfo>> { }
    private static class FileStringResponse extends TapisResponse<String> { }

    @Inject
    IFileUtilsService fileUtilsService;

    @GET
    @FileOpsAuthorization(permRequired = Permission.READ)
    @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Get stat information for a file or directory.",
               description = "Get stat information for a file or directory.",
               tags = {"file operations"})
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            content = @Content(schema = @Schema(implementation = FileStatInfoResponse.class)),
            description = "Stat information for the file or directory."),
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
    public Response getStatInfo(
        @Parameter(description = "System ID", required = true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
        @Parameter(description = "Path to a file or directory", required = false, example = EXAMPLE_PATH) @PathParam("path") String path,
        @Context SecurityContext securityContext) {
        String opName = "getStatInfo";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            Instant start = Instant.now();
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
            FileStatInfo fileStatInfo = fileUtilsService.getStatInfo(client, path);
            String msg = Utils.getMsgAuth("FILES_DURATION", user, opName, systemId, Duration.between(start, Instant.now()).toMillis());
            log.debug(msg);
            TapisResponse<FileStatInfo> resp = TapisResponse.createSuccessResponse("ok", fileStatInfo);
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
    @FileOpsAuthorization(permRequired = Permission.MODIFY)
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Run a native operation",
               description = "Run a native operation such as chmod, chown.",
               tags = {"file operations"})
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
    public Response runLinuxNativeOp(
        @Parameter(description = "System ID", required = true) @PathParam("systemId") String systemId,
        @Parameter(description = "Path to a file or directory", required = true) @PathParam("path") String path,
        @Valid NativeLinuxOpRequest request,
        @Context SecurityContext securityContext) {
        String opName = "runLinuxNativeOp";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
            if (client == null) {
                throw new NotFoundException(Utils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));
            }
            switch (request.getOperation()) {
              case CHMOD -> fileUtilsService.linuxChmod(client, path);
              case CHOWN -> fileUtilsService.linuxChown(client, path);
              case CHGRP -> fileUtilsService.linuxChgrp(client, path);
            }
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }
}
