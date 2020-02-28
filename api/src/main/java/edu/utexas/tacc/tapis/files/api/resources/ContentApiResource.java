package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.clients.*;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Objects;


@Path("/content")
public class ContentApiResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private Logger log = LoggerFactory.getLogger(ContentApiResource.class);

    @Inject FileOpsService fileOpsService;


    @GET
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Retrieve a file from the files service", description = "Get file contents/serve file", tags={ "content" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "OK")
    })
    public Response filesGetContents(
            @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true, example = EXAMPLE_PATH) @PathParam("path") String path,
            @Parameter(description = "Range of bytes to send", example = "range=0,999") @HeaderParam("range") HeaderByteRange range,
            @Parameter(description = "Send 1k of UTF-8 encoded string back starting at 'page' 1, ex more=1") @HeaderParam("more") Long moreStartPage,
            @Context SecurityContext securityContext) {
        try {

            InputStream stream;
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

            return Response
                    .ok(stream, mtype)
                    .header("content-disposition", contentDisposition)
                    .build();
        } catch (ServiceException ex) {
            log.error(ex.getMessage());
            throw new WebApplicationException();
        }
    }

}
