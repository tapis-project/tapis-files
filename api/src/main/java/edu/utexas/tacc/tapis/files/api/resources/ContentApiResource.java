package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.Min;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Objects;


/*
 * JAX-RS REST resource for Tapis File content downloads (file or directory)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
@Path("/v3/files/content")
public class ContentApiResource extends BaseFileOpsResource {

    private static final Logger log = LoggerFactory.getLogger(ContentApiResource.class);

    @Inject
    IFileOpsService fileOpsService;

    @GET
    @ManagedAsync
    @Path("/{systemId}/{path:.+}")
    public void getContents(@PathParam("systemId") String systemId,
                            @PathParam("path") String path,
                            @HeaderParam("range") HeaderByteRange range,
                            @QueryParam("zip") boolean zip,
                            @HeaderParam("more") @Min(1) Long moreStartPage,
                            @Context SecurityContext securityContext,
                            @Suspended final AsyncResponse asyncResponse)
    {
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
          // TODO/TBD move most of this logic and setup of client to the service layer.
          // TODO/TBD: replace checkSystemAndGetClient with getSystem
          //             or move fetch of system to svc layer also?
          getSystem(systemId, user, path);

            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            if (zip) {
               sendZip(asyncResponse, client, path, user, systemId);
            } else {
                // Ensure that the path is not a dir, if not zip, then this will error out
                if (path.endsWith("/")) {
                    throw new BadRequestException(Utils.getMsgAuth("FILES_CONT_BAD", user, systemId, path));
                }
                if (range != null) {
                    sendByteRange(asyncResponse, client, path, range, user, systemId);
                } else if (!Objects.isNull(moreStartPage)) {
                  sendText(asyncResponse, client, path, moreStartPage, user, systemId);
                } else {
                   sendFullStream(asyncResponse, client, path, user, systemId);
                }
            }
        } catch (ServiceException | IOException ex) {
            String msg = Utils.getMsgAuth("FILES_CONT_ERR", user, systemId, path, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }

    private void sendByteRange(AsyncResponse asyncResponse, IRemoteDataClient client, String path, HeaderByteRange range,
                               AuthenticatedUser user, String systemId) throws ServiceException, IOException {
        java.nio.file.Path filepath = Paths.get(path);
        String filename = filepath.getFileName().toString();
        StreamingOutput outStream = output -> {
            InputStream stream = null;
            try {
                stream = fileOpsService.getBytes(client, path, range.getMin(), range.getMax());
                stream.transferTo(output);
            } catch (NotFoundException ex) {
                throw ex;
            } catch (Exception e) {
                throw new WebApplicationException(Utils.getMsgAuth("FILES_CONT_ZIP_ERR", user, systemId, path), e);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        };
        String contentDisposition = String.format("attachment; filename=%s", filename);
        Response response = Response
            .ok(outStream, MediaType.TEXT_PLAIN)
            .header("content-disposition", contentDisposition)
            .header("cache-control", "max-age=3600")
            .build();
        asyncResponse.resume(response);
    }

    private void sendText(AsyncResponse asyncResponse, IRemoteDataClient client, String path, Long moreStartPage,
                          AuthenticatedUser user, String systemId) throws ServiceException, IOException {
        StreamingOutput outStream = output -> {
            InputStream stream = null;
            try {
                stream = fileOpsService.more(client, path, moreStartPage);
                stream.transferTo(output);
            } catch (NotFoundException ex) {
                throw ex;
            } catch (Exception e) {
                throw new WebApplicationException(Utils.getMsgAuth("FILES_CONT_ZIP_ERR", user, systemId, path), e);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        };
        String contentDisposition = "inline";
        Response response = Response
            .ok(outStream, MediaType.TEXT_PLAIN)
            .header("content-disposition", contentDisposition)
            .header("cache-control", "max-age=3600")
            .build();
        asyncResponse.resume(response);
    }

    private void sendZip(AsyncResponse asyncResponse, IRemoteDataClient client, String path, AuthenticatedUser user, String systemId) throws ServiceException, IOException {
        java.nio.file.Path filepath = Paths.get(path);
        String filename = filepath.getFileName().toString();
        StreamingOutput outStream = output -> {
            try {
                fileOpsService.getZip(client, output, path);
            } catch (NotFoundException ex) {
                throw ex;
            } catch (Exception e) {
                throw new WebApplicationException(Utils.getMsgAuth("FILES_CONT_ZIP_ERR", user, systemId, path), e);
            }
        };
        String newName = changeFileExtensionForZip(filename);
        String disposition = String.format("attachment; filename=%s", newName);
        Response resp =  Response.ok(outStream, MediaType.APPLICATION_OCTET_STREAM)
            .header("content-disposition", disposition)
            .build();
        asyncResponse.resume(resp);
    }


    private void sendFullStream(AsyncResponse asyncResponse, IRemoteDataClient client, String path, AuthenticatedUser user, String systemId) throws ServiceException, IOException {

        StreamingOutput outStream = output -> {
            InputStream stream = null;
            try {
                stream = fileOpsService.getStream(client, path);
                stream.transferTo(output);
            } catch (NotFoundException ex) {
                throw ex;
            } catch (Exception e) {
                throw new WebApplicationException(Utils.getMsgAuth("FILES_CONT_ZIP_ERR", user, systemId, path), e);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        };
        java.nio.file.Path filepath = Paths.get(path);
        String filename = filepath.getFileName().toString();
        String contentDisposition = String.format("attachment; filename=%s", filename);
        Response response = Response
            .ok(outStream, MediaType.APPLICATION_OCTET_STREAM)
            .header("content-disposition", contentDisposition)
            .header("cache-control", "max-age=3600")
            .build();
        asyncResponse.resume(response);
    }

    private String changeFileExtensionForZip(String name) {
        String filename = FilenameUtils.removeExtension(name);
        return filename + ".zip";
    }
}
