package edu.utexas.tacc.tapis.files.api.resources;

import com.google.gson.JsonSyntaxException;
import edu.utexas.tacc.tapis.files.api.models.PostItCreateRequest;
import edu.utexas.tacc.tapis.files.api.models.PostItUpdateRequest;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.PostIt;
import edu.utexas.tacc.tapis.files.lib.services.PostItRedeemContext;
import edu.utexas.tacc.tapis.files.lib.services.PostItsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import org.apache.commons.io.IOUtils;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@Path("/v3/files/postits")
public class PostItsResource {
    private static final Logger log = LoggerFactory.getLogger(PostItsResource.class);
    private final String className = getClass().getSimpleName();
    private static final String INVALID_JSON_INPUT = "NET_INVALID_JSON_INPUT";
    private static String POSTIT_CREATE_REQUEST="/edu/utexas/tacc/tapis/files/api/jsonschema/PostItCreateRequest.json";
    private static String POSTIT_UPDATE_REQUEST="/edu/utexas/tacc/tapis/files/api/jsonschema/PostItUpdateRequest.json";
    private static final String JSON_VALIDATION_ERR = "TAPIS_JSON_VALIDATION_ERROR";

    @Context
    private UriInfo uriInfo;
    @Context
    private SecurityContext securityContext;
    @Context
    private Request request;
    @Inject
    private PostItsService service;

    @POST
    @Path("/{systemId}/{path:(.*+)}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createPostIt(@PathParam("systemId") String systemId,
                                 @PathParam("path") String path,
                                 InputStream payloadStream) {

        ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        String opName = "createPostIt";
        String jsonString = getJsonString(payloadStream, opName);
        PostItCreateRequest createRequest = getJsonObjectFromString(jsonString,
                "createPostIt", POSTIT_CREATE_REQUEST, PostItCreateRequest.class);


        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "SystemId: ", systemId, "Path: ", path, jsonString);
        PostIt createdPostIt = null;
        try {
            createdPostIt = service.createPostIt(rUser, systemId, path, createRequest.getValidSeconds(),
                    createRequest.getAllowedUses());
            updateRedeemUrl(createdPostIt);
        } catch (TapisException | ServiceException ex) {
            String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_ERROR", rUser,
                    opName, systemId, path, ex.getMessage(), ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_COMPLETE", rUser,
                opName, systemId, path, createdPostIt.getId());
        TapisResponse<PostIt> tapisResponse = TapisResponse.createSuccessResponse(msg, createdPostIt);
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }

    @GET
    @Path("/{postItId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPostIt(@PathParam("postItId")String postItId) {
        String opName = "getPostIt";
        PostIt postIt = null;
        ResourceRequestUser rUser =
                new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "PostItId: ", postItId);
        try {
            postIt = service.getPostIt(rUser, postItId);
            updateRedeemUrl(postIt);
        } catch (TapisException | ServiceException ex) {
            String msg = LibUtils.getMsgAuthR("FAPI_POSTITS_OP_ERROR_ID", rUser,
                    opName, postItId, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_COMPLETE", rUser,
                opName, postIt.getSystemId(), postIt.getPath(), postIt.getId());
        TapisResponse<PostIt> tapisResponse = TapisResponse.createSuccessResponse(msg, postIt);
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response listPostIts() {
        String opName = "listPostIts";
        List<PostIt> postIts = Collections.emptyList();
        ResourceRequestUser rUser =
                new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString());

        try {
            postIts = service.listPostIts(rUser);
        } catch (TapisException | ServiceException ex) {
            String msg = LibUtils.getMsgAuthR("POSTITS_LIST_ERR", rUser, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth(
                "FAPI_POSTITS_LIST_COMPLETE", rUser, opName, postIts.size());
        TapisResponse<List<PostIt>> tapisResponse =
                TapisResponse.createSuccessResponse(msg, postIts.stream().map(postIt -> {
                    updateRedeemUrl(postIt);
                    return postIt;
                }).toList());
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }


    @PATCH
    @Path("/{postItId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updatePostIt(@PathParam("postItId") String postItId,
                                 InputStream payloadStream) {
        ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        String opName = "updatePostIt";
        String jsonString = getJsonString(payloadStream, opName);
        PostItUpdateRequest updateRequest = getJsonObjectFromString(jsonString,
                opName, POSTIT_UPDATE_REQUEST, PostItUpdateRequest.class);
        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "PostItId: ", postItId, jsonString);

        PostIt updatedPostIt = null;
        try {
            updatedPostIt = service.updatePostIt(rUser, postItId,
                    updateRequest.getValidSeconds(), updateRequest.getAllowedUses());
            updateRedeemUrl(updatedPostIt);
        } catch (TapisException | ServiceException ex) {
            String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_ERROR_ID", rUser, opName, postItId, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_COMPLETE", rUser, opName,
                updatedPostIt.getSystemId(), updatedPostIt.getPath(), updatedPostIt.getId());
        TapisResponse<PostIt> tapisResponse = TapisResponse.createSuccessResponse(msg, updatedPostIt);
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }

    @GET
    @ManagedAsync
    @PermitAll
    @Path("/redeem/{postItId}")
    public void redeemPostIt(@PathParam("postItId") String postItId,
                            @QueryParam("zip") Boolean zip,
                            @Suspended final AsyncResponse asyncResponse) {
        String opName = "redeemPostIt";
        PostItRedeemContext redeemContext = null;

        try {
            if (log.isTraceEnabled()) {
                String msg = ApiUtils.getMsg("FAPI_TRACE_REQUEST", null,
                        null, null, null, null, className, opName,
                        request.getRequestURL().toString(), "PostItId=" + postItId);
                log.trace(msg);
            }

            redeemContext = service.redeemPostIt(postItId, zip);

            // Build the response using the outStream, contentDisposition and mediaType
            // For some reason non-zip has cache-control max-age of 1 hour and zip has no header,
            //   presumably so if a very large directory takes more than 1 hour to zip up it will not time out.
            Response.ResponseBuilder responseBuilder =
                    Response.ok(redeemContext.getOutStream(), redeemContext.getMediaType()).
                            header("content-disposition", redeemContext.getContentDisposition());
            if (!redeemContext.isZip()) {
                responseBuilder.header("cache-control", "max-age=3600");
            }

            // Start the streaming response
            asyncResponse.resume(responseBuilder.build());
        } catch (TapisException ex) {
            String msg = LibUtils.getMsg("POSTITS_ERR_NOT_REDEEMABLE",
                    opName, postItId, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

    }

    private  String getJsonString(InputStream payloadStream, String opName) {
        String json;
        try {
            json = IOUtils.toString(payloadStream, StandardCharsets.UTF_8);
        } catch (Exception e) {
            String msg;
            msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName , e.getMessage());
            log.error(msg, e);
            throw new BadRequestException(msg);
        }
        return json;
    }

    private <T> T getJsonObjectFromString(String jsonString, String opName,
                                               String jsonSchemaFile, Class<T> jsonObjectClass) {
        // Create validator specification and validate the json against the schema
        JsonValidatorSpec spec = new JsonValidatorSpec(jsonString, jsonSchemaFile);
        try { JsonValidator.validate(spec); }
        catch (TapisJSONException e)
        {
            String msg;
            msg = MsgUtils.getMsg(JSON_VALIDATION_ERR, e.getMessage());
            log.error(msg, e);
            throw new BadRequestException(msg);
        }

        // deserialize the json string into an object
        T jsonObject;
        try {
            jsonObject = TapisGsonUtils.getGson().fromJson(jsonString, jsonObjectClass);
        }
        catch (JsonSyntaxException e)
        {
            String msg;
            msg = MsgUtils.getMsg(INVALID_JSON_INPUT, opName, e.getMessage());
            log.error(msg, e);
            throw new BadRequestException(msg);
        }

        return jsonObject;
    }

    private <T> T getJsonObjectFromInputStream(InputStream payloadStream, String opName,
                          String jsonSchemaFile, Class<T> jsonObjectClass) {
        String jsonString = getJsonString(payloadStream, opName);
        return getJsonObjectFromString(jsonString, opName, jsonSchemaFile, jsonObjectClass);
    }

    private void updateRedeemUrl(PostIt postIt) {
        // if we don't have enough info to build the redeemUrl, just return
        if((postIt == null) || (postIt.getId() == null)) {
            return;
        }
        URI redeemURI = uriInfo.getBaseUriBuilder().
                path("v3/files/postits/redeem").
                path(postIt.getId()).build();
        if(redeemURI != null) {
            postIt.setRedeemUrl(redeemURI.toString());
        }
    }
}
