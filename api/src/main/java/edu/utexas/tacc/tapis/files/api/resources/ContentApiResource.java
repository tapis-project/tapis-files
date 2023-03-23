package edu.utexas.tacc.tapis.files.api.resources;

import java.nio.file.Paths;
import java.util.Objects;
import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.validation.constraints.Min;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * JAX-RS REST resource for Tapis File content downloads (file or directory)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
@Path("/v3/files/content")
public class ContentApiResource extends BaseFileOpsResource
{
  private static final Logger log = LoggerFactory.getLogger(ContentApiResource.class);
  private final String className = getClass().getSimpleName();

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************
  @Context
  private HttpHeaders _httpHeaders;
  @Context
  private Application _application;
  @Context
  private UriInfo _uriInfo;
  @Context
  private SecurityContext _securityContext;
  @Context
  private ServletContext _servletContext;
  @Context
  private Request _request;

  @Inject
  FileOpsService fileOpsService;

  /**
   * Start an async data stream that can be used to receive contents of a file
   * This is broken into two methods with a common method to start the download because of problems with
   *   downloading the "root" directory. The pattern "/{systemId}/{path:(.*+)}" does not work when requester
   *   attempts to download "/{systemId}"
   * @param systemId - id of system
   * @param path - path on system relative to system rootDir
   * @param range - range of bytes to include in the stream
   * @param zip - flag indicating if a zip stream should be created
   * @param startPage -
   * @param impersonationId - use provided Tapis username instead of oboUser when checking auth, getSystem (effUserId)
   * @param sharedAppCtx - Indicates that request is part of a shared app context. Tapis auth bypassed.
   * @param securityContext - user identity
   */
  @GET
  @ManagedAsync
  @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  public void getContents(@PathParam("systemId") String systemId,
                          @PathParam("path") String path,
                          @HeaderParam("range") HeaderByteRange range,
                          @QueryParam("zip") @DefaultValue("false") boolean zip,
                          @HeaderParam("more") @Min(1) Long startPage,
                          @QueryParam("impersonationId") String impersonationId,
                          @QueryParam("sharedAppCtx") @DefaultValue("false") boolean sharedAppCtx,
                          @Context SecurityContext securityContext,
                          @Suspended final AsyncResponse asyncResponse)
  {
    String opName = "getContents";
    downloadPath(opName, systemId, path, range, zip, startPage, impersonationId, sharedAppCtx, securityContext, asyncResponse);
  }

  @GET
  @ManagedAsync
  @Path("/{systemId}")
  public void getContentsRoot(@PathParam("systemId") String systemId,
                              @HeaderParam("range") HeaderByteRange range,
                              @QueryParam("zip") @DefaultValue("false") boolean zip,
                              @HeaderParam("more") @Min(1) Long startPage,
                              @QueryParam("impersonationId") String impersonationId,
                              @QueryParam("sharedAppCtx") @DefaultValue("false") boolean sharedAppCtx,
                              @Context SecurityContext securityContext,
                              @Suspended final AsyncResponse asyncResponse)
  {
    String opName = "getContents";
    downloadPath(opName, systemId, "", range, zip, startPage, impersonationId, sharedAppCtx, securityContext, asyncResponse);
  }

  // ************************************************************************
  // *********************** Private Methods ********************************
  // ************************************************************************

  /*
   * Common routine to download
   */
  private void downloadPath(String opName, String systemId, String path, HeaderByteRange range, boolean zip,
                            Long startPage, String impersonationId, boolean sharedAppCtx,
                            SecurityContext securityContext, AsyncResponse asyncResponse)
  {
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
//    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
//    // Utility method returns null if all OK and appropriate error response if there was a problem.
//    // NOTE: This causes TestContentsRoutes to fail, but Routes tests work for Ops and Txfrs
//    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
// ...

    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
              "path="+path, "range="+range, "zip="+zip, "more="+startPage, "impersonationId="+impersonationId,
              "sharedCtx="+sharedAppCtx);

    // TODO REMOVE
    //      Convert incoming boolean sharedAppCtx true/false into "true" / null for sharedCtxGrantor
    //      From now on we can deal with strings.
    String sharedCtxGrantor = sharedAppCtx ? Boolean.toString(sharedAppCtx) : null;

    // Get system. This requires READ permission.
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId, impersonationId, sharedCtxGrantor);

    // ---------------------------- Make service calls to start data streaming -------------------------------
    // Note that we do not use try/catch around service calls because exceptions are already either
    //   a WebApplicationException or some other exception handled by the mapper that converts exceptions
    //   to responses (ApiExceptionMapper).
    String contentDisposition = null;
    StreamingOutput outStream = null;
    String mediaType = null;
    // Determine the target file name to use in ContentDisposition (.zip will get added for zipStream)
    java.nio.file.Path inPath = Paths.get(path);
    java.nio.file.Path  filePath = inPath.getFileName();
    String fileName = (filePath == null) ? "" : filePath.getFileName().toString();
    if(StringUtils.isBlank(fileName) || fileName.equals("/")) {
      fileName = "systemRoot";
    }

    // Make a different service call depending on type of response:
    //  - zipStream, byteRangeStream, paginatedStream, fullStream
    if (zip)
    {
      // Send a zip stream. This can handle a path ending in /
      outStream = fileOpsService.getZipStream(rUser, sys, path, impersonationId, sharedCtxGrantor);
      String newName = FilenameUtils.removeExtension(fileName) + ".zip";
      contentDisposition = String.format("attachment; filename=%s", newName);
      mediaType = MediaType.APPLICATION_OCTET_STREAM;
    }
    else
    {
      // Make sure the requested path is not a directory
      FileInfo fileInfo = fileOpsService.getFileInfo(rUser, sys, path, impersonationId, sharedCtxGrantor);
      if (fileInfo == null)
      {
        throw new NotFoundException(LibUtils.getMsgAuth("FILES_CONT_NO_FILEINFO", user, systemId, path));
      }
      if (fileInfo.isDir())
      {
        throw new BadRequestException(LibUtils.getMsgAuth("FILES_CONT_DIR_NOZIP", user, systemId, path));
      }
      // Send a byteRange, page blocks or the full stream.
      if (range != null)
      {
        outStream = fileOpsService.getByteRangeStream(rUser, sys, path, range, impersonationId, sharedCtxGrantor);
        contentDisposition = String.format("attachment; filename=%s", fileName);
        mediaType = MediaType.TEXT_PLAIN;
      }
      else if (!Objects.isNull(startPage))
      {
        outStream = fileOpsService.getPagedStream(rUser, sys, path, startPage, impersonationId, sharedCtxGrantor);
        contentDisposition = "inline";
        mediaType = MediaType.TEXT_PLAIN;
      }
      else
      {
        outStream = fileOpsService.getFullStream(rUser, sys, path, impersonationId, sharedCtxGrantor);
        contentDisposition = String.format("attachment; filename=%s", fileName);
        mediaType = MediaType.APPLICATION_OCTET_STREAM;
      }
    }

    // Build the response using the outStream, contentDisposition and mediaType
    // For some reason non-zip has cache-control max-age of 1 hour and zip has no header,
    //   presumably so if a very large directory takes more than 1 hour to zip up it will not time out.
    Response response;
    if (zip)
    {
      response = Response.ok(outStream, mediaType)
              .header("content-disposition", contentDisposition)
              .build();
    }
    else
    {
      response = Response.ok(outStream, mediaType)
              .header("content-disposition", contentDisposition)
              .header("cache-control", "max-age=3600")
              .build();
    }
    // Start the streaming response
// In case we want a timeout, we can use something like this:
//    asyncResponse.setTimeout(1, TimeUnit.DAYS);
    asyncResponse.resume(response);
  }
}
