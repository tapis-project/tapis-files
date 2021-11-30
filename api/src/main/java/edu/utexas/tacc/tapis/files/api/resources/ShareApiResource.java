package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import edu.utexas.tacc.tapis.files.lib.models.SharedFileObject;
import edu.utexas.tacc.tapis.files.api.models.ShareFileRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*TODO
 * JAX-RS REST resource for Tapis File share operations
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 */
//@Path("/v3/files/share")
public class ShareApiResource  {

    private static final Logger log = LoggerFactory.getLogger(ShareApiResource.class);

//    @DELETE
//    @Path("/{systemId}/{path}")
//
//    @Produces(MediaType.APPLICATION_JSON)
//    @Operation(summary = "Revoke a shared file resource ", description = "Removes any outstanding shares on a file resource. ", tags={ "share" })
//    @ApiResponses(value = {
//        @ApiResponse(responseCode = "200", description = "Shared file object",
//            content = @Content(schema = @Schema(implementation = SharedFileObject.class)))
//    })
//    public Response shareDelete(
//        @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
//        @Parameter(description = "path",required=true) @PathParam("path") String path,
//        @Context SecurityContext securityContext) throws NotFoundException {
//        String opName = "unshare";
//        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
//        try {
//          // Use if (log != null) so it will compile
//          if (log != null) throw new ServiceException("Operation not yet supported.");
//        } catch (ServiceException ex) {
//          String msg = Utils.getMsgAuth("FILESAPI_SHARE_ERROR", user, systemId, opName, ex.getMessage());
//          log.error(msg, ex);
//          throw new WebApplicationException(msg, ex);
//        }
//        return Response.ok().build();
//    }
//
//
//    @GET
//    @Path("/{systemId}/{path}")
//
//    @Produces(MediaType.APPLICATION_JSON)
//    @Operation(summary = "List the shares on a file resource. ", description = "List all shares on a given file resource. ", tags={ "share" })
//    @ApiResponses(value = {
//        @ApiResponse(
//            responseCode = "200",
//            description = "List of shares",
//            content = @Content(array = @ArraySchema(schema = @Schema(implementation = SharedFileObject.class))))
//    })
//    public Response shareList (
//        @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
//        @Parameter(description = "path",required=true) @PathParam("path") String path,
//        @Context SecurityContext securityContext) throws NotFoundException {
//        String opName = "listShares";
//        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
//        try {
//          // Use if (log != null) so it will compile
//          if (log != null) throw new ServiceException("Operation not yet supported.");
//        } catch (ServiceException ex) {
//          String msg = Utils.getMsgAuth("FILESAPI_SHARE_ERROR", user, systemId, opName, ex.getMessage());
//          log.error(msg, ex);
//          throw new WebApplicationException(msg, ex);
//        }
//        return Response.ok().build();
//    }
//
//
//    @POST
//    @Path("/{systemId}/{path}")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @Produces(MediaType.APPLICATION_JSON)
//    @Operation(
//        summary = "Grant temporary access to a file resource. ",
//        description = "Creates a link that is valid for the requested validity time for the given user for the resource in {systemId} at path {path} ",
//        tags={ "share" })
//    @ApiResponses(value = {
//        @ApiResponse(
//            responseCode = "200",
//            description = "Shared file object",
//            content = @Content(schema = @Schema(implementation = SharedFileObject.class)))
//    })
//    public Response shareFile (
//        @Parameter(description = "System ID",required=true) @PathParam("systemId") String systemId,
//        @Parameter(description = "path",required=true) @PathParam("path") String path,
//        @Parameter(description = "" ) ShareFileRequest body,
//        @Context SecurityContext securityContext) throws NotFoundException {
//        String opName = "share";
//        AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
//        try {
//          // Use if (log != null) so it will compile
//          if (log != null) throw new ServiceException("Operation not yet supported.");
//        } catch (ServiceException ex) {
//          String msg = Utils.getMsgAuth("FILESAPI_SHARE_ERROR", user, systemId, opName, ex.getMessage());
//          log.error(msg, ex);
//          throw new WebApplicationException(msg, ex);
//        }
//
//        // Add row to security kernel?
//        return Response.ok().build();
//    }
}