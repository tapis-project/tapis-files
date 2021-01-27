package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.files.api.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthorization;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Objects;


@Path("/v3/files/content")
public class ContentApiResource extends BaseFilesResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private static final Logger log = LoggerFactory.getLogger(ContentApiResource.class);

    @GET
    @ManagedAsync
    @FileOpsAuthorization(permsRequired = FilePermissionsEnum.READ)
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Retrieve a file from the files service", description = "Get file contents/serve file", tags={ "content" })
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "401", description = "Not Authenticated"),
        @ApiResponse(responseCode = "404", description = "Not Found"),
        @ApiResponse(responseCode = "403", description = "Not Authorized")
    })
    public void filesGetContents(
            @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true, example = EXAMPLE_PATH) @PathParam("path") String path,
            @Parameter(description = "Range of bytes to send", example = "range=0,999") @HeaderParam("range") HeaderByteRange range,
            @Parameter(description = "Send 1k of UTF-8 encoded string back starting at 'page' 1, ex more=1") @Min(1) @HeaderParam("more") Long moreStartPage,
            @Context SecurityContext securityContext,
            @Suspended final AsyncResponse asyncResponse) {

        InputStream stream = null;
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();

        try {
            TSystem system = systemsCache.getSystem(user.getTenantId(), systemId, user.getName());
            String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
            IFileOpsService fileOpsService = makeFileOpsService(system, effectiveUserId);
            String mtype = MediaType.APPLICATION_OCTET_STREAM;
            String contentDisposition;

            // Ensure that the path is not a dir?
            if (path.endsWith("/")) {
                throw new BadRequestException("Only files can be served.");
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

//            InputStream finalStream = stream;
//            StreamingOutput outStream = output -> {
//                try {
//                    IOUtils.copy(finalStream, output);
//                } catch (Exception e) {
//                    throw new WebApplicationException(e);
//                }
//            };

            Response response =  Response
                    .ok(stream, mtype)
                    .header("content-disposition", contentDisposition)
                    .header("cache-control", "max-age=3600")
                    .build();
            asyncResponse.resume(response);
        } catch (TapisClientException ex) {
            throw new BadRequestException("Only files can be served");
        } catch (ServiceException | IOException ex) {
            throw new WebApplicationException(ex);
        }
    }

}
