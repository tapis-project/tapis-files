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
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/*
 * JAX-RS REST resource for Tapis File operations (list, mkdir, delete, moveCopy, upload)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
@Path("/v3/files/ops")
public class OperationsApiResource extends BaseFileOpsResource
{
    private static final int MAX_RECURSION_DEPTH = 10;
    private static final Logger log = LoggerFactory.getLogger(OperationsApiResource.class);

    @Inject
    IFileOpsService fileOpsService;

  /**
   * List files at path
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param limit - pagination limit
   * @param offset - pagination offset
   * @param recurse - flag indicating a recursive listing should be provided
   * @param securityContext - user identity
   * @return response containing list of files
   */
    @GET
    @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
    @Produces(MediaType.APPLICATION_JSON)
    public Response listFiles(@PathParam("systemId") String systemId,
                              @PathParam("path") String path,
                              @QueryParam("limit") @DefaultValue("1000") @Max(1000) int limit,
                              @QueryParam("offset") @DefaultValue("0") @Min(0) long offset,
                              @QueryParam("recurse") @DefaultValue("false") boolean recurse,
                              @Context SecurityContext securityContext)
    {
        String opName = "listFiles";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        log.debug("PATH={}", path);
        try {
            Instant start = Instant.now();
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            List<FileInfo> listing;
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

  /**
   * Upload a file
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param fileInputStream - stream of data to place in the file
   * @param securityContext - user identity
   * @return response
   */
    @POST
    @Path("/{systemId}/{path:.+}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response upload(@PathParam("systemId") String systemId,
                           @PathParam("path") String path,
                           @FormDataParam(value = "file") InputStream fileInputStream,
                           @Context SecurityContext securityContext)
    {
        String opName = "upload";
        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
        try {
            IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
            fileOpsService.upload(client, path, fileInputStream);
            TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok", "ok");
            return Response.ok(resp).build();
        } catch (ServiceException | IOException e) {
            String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
            log.error(msg, e);
            throw new WebApplicationException(msg, e);
        }
    }

  /**
   * Create a directory
   * @param systemId - id of system
   * @param mkdirRequest - request body containing a path relative to system rootDir
   * @param securityContext - user identity
   * @return response
   */
    @POST
    @Path("/{systemId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response mkdir(@PathParam("systemId") String systemId,
                          @Valid MkdirRequest mkdirRequest,
                          @Context SecurityContext securityContext)
    {
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

  /**
   *
   * @param systemId - id of system
   * @param path - source path on system
   * @param request - request body containing operation (MOVE/COPY) and target path
   * @param securityContext - user identity
   * @return response
   */
    @PUT
    @Path("/{systemId}/{path:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response moveCopy(@PathParam("systemId") String systemId,
                             @PathParam("path") String path,
                             @Valid MoveCopyRequest request,
                             @Context SecurityContext securityContext)
    {
      String opName = "moveCopy";
      AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
      try
      {
        // Get a remoteDataClient for the given TapisSystem
        IRemoteDataClient client = checkSystemAndGetClient(systemId, user, path);
        if (client == null) throw new NotFoundException(Utils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));
        // Perform the operation
        MoveCopyOperation operation = request.getOperation();
        if (operation.equals(MoveCopyOperation.MOVE)) fileOpsService.move(client, path, request.getNewPath());
        else if (operation.equals(MoveCopyOperation.COPY)) fileOpsService.copy(client, path, request.getNewPath());
        // Return response
        TapisResponse<String> resp = TapisResponse.createSuccessResponse("ok");
        return Response.ok(resp).build();
      }
      catch (ServiceException | IOException e)
      {
        String msg = Utils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
        log.error(msg, e);
        throw new WebApplicationException(msg, e);
      }
    }

  /**
   * Delete a directory
   * @param systemId - id of system
   * @param path - directory to delete
   * @param securityContext - user identity
   * @return response
   */
    @DELETE
    @Path("/{systemId}/{path:(.*+)}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(@PathParam("systemId") String systemId,
                           @PathParam("path") String path,
                           @Context SecurityContext securityContext)
    {
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
