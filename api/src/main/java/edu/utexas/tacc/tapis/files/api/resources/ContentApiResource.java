package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.clients.*;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.inject.Inject;
import javax.validation.ValidationException;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;


@Path("/content")
public class ContentApiResource {

    private static final String EXAMPLE_SYSTEM_ID = "system123";
    private static final String EXAMPLE_PATH = "/folderA/folderB/";
    private RemoteDataClientFactory clientFactory = new RemoteDataClientFactory();
    private Logger log = LoggerFactory.getLogger(ContentApiResource.class);

    @Inject FileOpsService fileOpsService;

    private static class ByteRange {
        private long min;
        private long max;
        private ByteRange(long min, long max) throws ValidationException {
            if (max < min) {
                throw new ValidationException("invalid range.");
            }
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public void setMin(long min) {
            this.min = min;
        }

        public long getMax() {
            return max;
        }

        public void setMax(long max) {
            this.max = max;
        }

        public static ByteRange fromParams(String qparms) {
            // should look like "0,1000"
            String[] params = qparms.split(",");

            return new ByteRange(Long.parseLong(params[0]) , Long.parseLong(params[1]));

        }
    }

    private ByteRange parseValidateRange(String input) {
        return ByteRange.fromParams(input);
    }

    @GET
    @Path("/{systemId}/{path:.+}")
    @Operation(summary = "Retrieve a file from the files service", description = "Get file contents/serve file", tags={ "content" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "OK")
    })
    public Response filesGetContents(
            @Parameter(description = "System ID",required=true, example = EXAMPLE_SYSTEM_ID) @PathParam("systemId") String systemId,
            @Parameter(description = "File path",required=true, example = EXAMPLE_PATH) @PathParam("path") String path,
            @Parameter(description = "Range of bytes to send", example = "range=0,999") @HeaderParam("range") String range,
            @Context SecurityContext securityContext) {
        try {

            InputStream stream;
            // Ensure that the path is not a dir?
            if (path.endsWith("/")) {
                throw new BadRequestException("Only files can be served.");
            }

            if (range != null) {
                ByteRange brange = ByteRange.fromParams(range);
                stream = fileOpsService.getBytes(path, brange.getMin(), brange.getMax());
            } else {
                stream = fileOpsService.getStream(path);
            }

            java.nio.file.Path filepath = Paths.get(path);
            String filename = filepath.getFileName().toString();

            return Response
                    .ok(stream, MediaType.APPLICATION_OCTET_STREAM)
                    .header("content-disposition", String.format("attachment; filename=%s", filename))
                    .build();
        } catch (ServiceException ex) {
            log.error(ex.getMessage());
            throw new WebApplicationException();
        }
    }

}
