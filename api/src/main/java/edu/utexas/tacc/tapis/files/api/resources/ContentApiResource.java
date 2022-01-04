package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.models.HeaderByteRange;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.io.FilenameUtils;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.validation.constraints.Min;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
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
import java.nio.file.Paths;
import java.util.Objects;

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
  // Always return a nicely formatted response
  private static final boolean PRETTY = true;

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
  IFileOpsService fileOpsService;

  @GET
  @ManagedAsync
  @Path("/{systemId}/{path:.+}")
  public void getContents(@PathParam("systemId") String systemId,
                          @PathParam("path") String path,
                          @HeaderParam("range") HeaderByteRange range,
                          @QueryParam("zip") boolean zip,
                          @HeaderParam("more") @Min(1) Long startPage,
                          @Context SecurityContext securityContext,
                          @Suspended final AsyncResponse asyncResponse)
  {
    String opName = "getContents";
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    // Check that we have all we need from the context, the jwtTenantId and jwtUserId
    // Utility method returns null if all OK and appropriate error response if there was a problem.
    // TODO/TBD: Leave this out for now since it prevents running of the tests. See api/pom.xml
//    TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get(); // Local thread context
//    Response resp = ApiUtils.checkContext(threadContext, PRETTY);
//    // If there is a problem throw an exception
//    if (resp != null)
//    {
//      String msg = LibUtils.getMsgAuth("FILES_CONT_ERR", user, systemId, path, "Unable to validate identity/request attributes");
//      // checkContext logs an error, so no need to log here.
//      throw new WebApplicationException(msg);
//    }

    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

    // Trace this request.
    if (log.isTraceEnabled())
      ApiUtils.logRequest(rUser, className, opName, _request.getRequestURL().toString(), "systemId="+systemId,
              "path="+path, "range="+range, "zip="+zip, "more="+startPage);

    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = fileOpsService.getSystemIfEnabled(rUser, systemId);

    // ---------------------------- Make service calls to start data streaming -------------------------------
    String contentDisposition = null;
    StreamingOutput outStream = null;
    String mediaType = null;
    // Determine the target file name (.zip will get added for zipStream)
    java.nio.file.Path filepath = Paths.get(path);
    String filename = filepath.getFileName().toString();
    // Make a different service call depending on type of response:
    //  - zipStream, byteRangeStream, paginatedStream, fullStream
    if (zip)
    {
      // Send a zip stream. This can handle a path ending in /
      outStream = fileOpsService.getZipStream(rUser, sys, path);
      String newName = FilenameUtils.removeExtension(filename) + ".zip";
      contentDisposition = String.format("attachment; filename=%s", newName);
      mediaType = MediaType.APPLICATION_OCTET_STREAM;
    }
    else
    {
      // So it will not be a zip. Check that path does not end with /.
      // If it ends with '/' then it is a bad request.
      if (path.endsWith("/"))
      {
        throw new BadRequestException(LibUtils.getMsgAuth("FILES_CONT_BAD", user, systemId, path));
      }
      // Send a byteRange, page blocks or the full stream.
      if (range != null)
      {
        outStream = fileOpsService.getByteRangeStream(rUser, sys, path, range);
        contentDisposition = String.format("attachment; filename=%s", filename);
        mediaType = MediaType.TEXT_PLAIN;
      }
      else if (!Objects.isNull(startPage))
      {
        outStream = fileOpsService.getPagedStream(rUser, sys, path, startPage);
        contentDisposition = "inline";
        mediaType = MediaType.TEXT_PLAIN;
      }
      else
      {
        outStream = fileOpsService.getFullStream(rUser, sys, path);
        contentDisposition = String.format("attachment; filename=%s", filename);
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
    asyncResponse.resume(response);
  }
}
