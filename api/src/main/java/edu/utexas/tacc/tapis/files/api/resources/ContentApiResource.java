package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.api.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthorization;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.inject.Inject;
import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Objects;


@Path("/v3/files/content")
public class ContentApiResource extends BaseFilesResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private static final Logger log = LoggerFactory.getLogger(ContentApiResource.class);

    @Inject
    SystemsClient systemsClient;

    @Inject
    RemoteDataClientFactory remoteDataClientFactory;

    @GET
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.READ)
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Retrieve a file from the files service", description = "Get file contents/serve file", tags={ "content" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "OK")
    })
    public Response filesGetContents(
            @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true, example = EXAMPLE_PATH) @PathParam("path") String path,
            @Parameter(description = "Range of bytes to send", example = "range=0,999") @HeaderParam("range") HeaderByteRange range,
            @Parameter(description = "Send 1k of UTF-8 encoded string back starting at 'page' 1, ex more=1") @Min(1) @HeaderParam("more") Long moreStartPage,
            @Context SecurityContext securityContext) {
        try {
            AuthenticatedUser user  = (AuthenticatedUser) securityContext.getUserPrincipal();
            TSystem system = systemsClient.getSystemByName(systemId);
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(system, user.getOboUser());
            FileOpsService fileOpsService = new FileOpsService(client);
            InputStream stream;
            String mtype = MediaType.APPLICATION_OCTET_STREAM;
            String contentDisposition;

            // Ensure that the path is not a dir?
            if (path.endsWith("/")) {
                throw new TapisException("Only files can be served.");
            }

            java.nio.file.Path filepath = Paths.get(path);
            String filename = filepath.getFileName().toString();
            contentDisposition = String.format("attachment; filename=%s", filename);

            if (range != null) {
                stream = fileOpsService.getBytes(path, range.getMin(), range.getMax());
            } else if (!Objects.isNull(moreStartPage)) {
                mtype = MediaType.TEXT_PLAIN;
                stream = fileOpsService.more(path, moreStartPage);
                contentDisposition = "inline";
            }
            else {
                stream = fileOpsService.getStream(path);
            }
            return Response
                    .ok(stream, mtype)
                    .header("content-disposition", contentDisposition)
                    .build();
        } catch (TapisException ex) {
            throw new BadRequestException("Only files can be served");
        } catch (NotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            log.error("ERROR: filesGetContents", ex);
            throw new WebApplicationException();
        }
    }

}
