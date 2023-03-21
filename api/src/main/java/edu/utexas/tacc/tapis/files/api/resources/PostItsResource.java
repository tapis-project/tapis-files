package edu.utexas.tacc.tapis.files.api.resources;

import com.google.gson.JsonSyntaxException;
import edu.utexas.tacc.tapis.files.api.models.PostItCreateRequest;
import edu.utexas.tacc.tapis.files.api.models.PostItUpdateRequest;
import edu.utexas.tacc.tapis.files.api.responses.DTOResponseBuilder;
import edu.utexas.tacc.tapis.files.api.responses.PostItDTO;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.PostIt;
import edu.utexas.tacc.tapis.files.lib.services.PostItRedeemContext;
import edu.utexas.tacc.tapis.files.lib.services.PostItsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.SearchParameters;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.util.MimeType;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import javax.ws.rs.core.UriBuilder;
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
    private static Integer POSTIT_LIST_DEFAULT_LIMIT=Integer.valueOf(100);
    TenantManager tenantManager = TenantManager.getInstance(RuntimeSettings.get().getTenantsServiceURL());

    private class ChangeResult {
        private final int changes;

        public ChangeResult(int changes) {
            this.changes = changes;
        }

        public int getChanges() {
            return changes;
        }
    }

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

        String opName = "createPostIt";
        ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        String jsonString = getJsonString(payloadStream, opName);
        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "SystemId: ", systemId, "Path: ", path, jsonString);

        PostItCreateRequest createRequest = new PostItCreateRequest();
        if(!StringUtils.isBlank(jsonString)) {
            createRequest = getJsonObjectFromString(jsonString,
                    "createPostIt", POSTIT_CREATE_REQUEST, PostItCreateRequest.class);
        }
        PostItDTO createdPostIt = null;
        try {
            PostIt postIt = service.createPostIt(rUser, systemId, path, createRequest.getValidSeconds(),
                    createRequest.getAllowedUses());
            createdPostIt = new PostItDTO(postIt);
            updateRedeemUrl(createdPostIt, rUser, opName);
        } catch (TapisException | ServiceException ex) {
            String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_ERROR", rUser,
                    opName, systemId, path, ex.getMessage(), ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_COMPLETE", rUser,
                opName, systemId, path, createdPostIt.getId());
        TapisResponse<PostItDTO> tapisResponse = TapisResponse.createSuccessResponse(msg, createdPostIt);
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }

    @GET
    @Path("/{postItId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPostIt(@PathParam("postItId") String postItId) {
        String opName = "getPostIt";
        PostItDTO postItDto = null;
        ResourceRequestUser rUser =
                new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "PostItId: ", postItId);
        try {
            PostIt retrievedPostIt = service.getPostIt(rUser, postItId);
            postItDto = new PostItDTO(retrievedPostIt);
            updateRedeemUrl(postItDto, rUser, opName);
        } catch (TapisException | ServiceException ex) {
            String msg = LibUtils.getMsgAuthR("FAPI_POSTITS_OP_ERROR_ID", rUser,
                    opName, postItId, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_COMPLETE", rUser,
                opName, postItDto.getSystemId(), postItDto.getPath(), postItDto.getId());
        TapisResponse<PostItDTO> tapisResponse = TapisResponse.createSuccessResponse(msg, postItDto);
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response listPostIts(@QueryParam("listType") @DefaultValue("OWNED") String listType) {
        String opName = "listPostIts";
        List<PostItDTO> postItDtos = Collections.emptyList();
        ResourceRequestUser rUser =
                new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());

        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString());

        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        Response response = ApiUtils.checkContext(threadContext, true);
        if(response != null) {
            return response;
        }

        SearchParameters searchParameters = threadContext.getSearchParameters();

        if(!EnumUtils.isValidEnum(PostItsService.ListType.class, listType)) {
            String msg = ApiUtils.getMsgAuth("FAPI_LISTTYPE_ERROR", rUser, listType);
            log.error(msg);
            throw new BadRequestException(msg);
        }

        PostItsService.ListType listTypeEnum = PostItsService.ListType.valueOf(listType);
        try {
            if(searchParameters.getLimit() == null) {
                searchParameters.setLimit(POSTIT_LIST_DEFAULT_LIMIT);
            }
            List<PostIt> postIts = service.listPostIts(rUser, listTypeEnum, searchParameters.getLimit(),
                    searchParameters.getOrderByList(), searchParameters.getSkip(),
                    searchParameters.getStartAfter());
            postItDtos = postIts.stream().map(postIt -> {
                PostItDTO postItDTO = new PostItDTO(postIt);
                updateRedeemUrl(postItDTO, rUser, opName);
                return postItDTO;
            }).toList();
            DTOResponseBuilder<PostItDTO> responseBuilder =
                    new DTOResponseBuilder<>(PostItDTO.class, searchParameters.getSelectList());

            String msg = ApiUtils.getMsgAuth(
                    "FAPI_POSTITS_LIST_COMPLETE", rUser, opName, postItDtos.size());

            return responseBuilder.createSuccessResponse(Response.Status.OK, msg, postItDtos);
        } catch (TapisException | ServiceException ex) {
            String msg = LibUtils.getMsgAuthR("POSTITS_LIST_ERR", rUser, opName, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }
    }


    @POST
    @Path("/{postItId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postUpdatePostIt(@PathParam("postItId") String postItId,
                                 InputStream payloadStream) {
        return updatePostit(postItId, payloadStream);
    }

    @PATCH
    @Path("/{postItId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response patchUpdatePostIt(@PathParam("postItId") String postItId,
                                 InputStream payloadStream) {
        return updatePostit(postItId, payloadStream);
    }

    public Response updatePostit(String postItId, InputStream payloadStream) {
        ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        String opName = "updatePostIt";
        String jsonString = getJsonString(payloadStream, opName);
        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "PostItId: ", postItId, jsonString);

        PostItUpdateRequest updateRequest = getJsonObjectFromString(jsonString,
                opName, POSTIT_UPDATE_REQUEST, PostItUpdateRequest.class);

        PostItDTO updatedPostIt = null;
        try {
            PostIt postIt = service.updatePostIt(rUser, postItId,
                    updateRequest.getValidSeconds(), updateRequest.getAllowedUses());
            updatedPostIt = new PostItDTO(postIt);
            updateRedeemUrl(updatedPostIt, rUser, opName);
        } catch (TapisException | ServiceException ex) {
            String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_ERROR_ID", rUser, opName, postItId, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_COMPLETE", rUser, opName,
                updatedPostIt.getSystemId(), updatedPostIt.getPath(), updatedPostIt.getId());
        TapisResponse<PostItDTO> tapisResponse = TapisResponse.createSuccessResponse(msg, updatedPostIt);
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
    }
    @GET
    @ManagedAsync
    @PermitAll
    @Path("/redeem/{postItId}")
    public void redeemPostIt(@PathParam("postItId") String postItId,
                            @QueryParam("zip") Boolean zip,
                            @QueryParam("download") @DefaultValue("false") Boolean download,
                            @Suspended final AsyncResponse asyncResponse) {
        String opName = "redeemPostIt";
        PostItRedeemContext redeemContext = null;

        try {
            if (log.isTraceEnabled()) {
                // The four nulls represent the token user/tenant etc.
                String msg = ApiUtils.getMsg("FAPI_TRACE_REQUEST", null,
                        null, null, null, null, className, opName,
                        request.getRequestURL().toString(), "PostItId=" + postItId,
                        "zip=", zip, "download=", download);
                log.trace(msg);
            }

            redeemContext = service.redeemPostIt(postItId, zip);

            // Build the response using the outStream, contentDisposition and mediaType
            // For some reason non-zip has cache-control max-age of 1 hour and zip has no header,
            //   presumably so if a very large directory takes more than 1 hour to zip up it will not time out.
            Response.ResponseBuilder responseBuilder = null;
            String filename = redeemContext.getFilename();
            if(download) {
                responseBuilder = Response.ok(redeemContext.getOutStream(), MediaType.APPLICATION_OCTET_STREAM)
                        .header("content-disposition", "attachment; filename=" + redeemContext.getFilename());
            } else {
                responseBuilder = Response.ok(redeemContext.getOutStream(), MimeType.getByFilename(filename))
                        .header("content-disposition", "inline; filename=" + filename);
            }

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

    @DELETE
    @Path("/{postItId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deletePostIt(@PathParam("postItId") String postItId) {
        String opName = "deletePostIt";
        int deleteCount = 0;
        ResourceRequestUser rUser =
                new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
        ApiUtils.logRequest(rUser, className, opName, request.getRequestURL().toString(),
                "PostItId: ", postItId);
        try {
            deleteCount = service.deletePostIt(rUser, postItId);
        } catch (TapisException | ServiceException ex) {
            String msg = LibUtils.getMsgAuthR("FAPI_POSTITS_OP_ERROR_ID", rUser,
                    opName, postItId, ex.getMessage());
            log.error(msg, ex);
            throw new WebApplicationException(msg, ex);
        }

        String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_DELETE_COMPLETE", rUser,
                opName, postItId);
        TapisResponse<ChangeResult> tapisResponse =
                TapisResponse.createSuccessResponse(msg, new ChangeResult(deleteCount));
        return Response.status(Response.Status.OK).entity(tapisResponse).build();
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

    private void updateRedeemUrl(PostItDTO postItDto, ResourceRequestUser rUser, String opName) {
        // if we don't have enough info to build the redeemUrl, just return
        if ((postItDto == null) || (postItDto.getId() == null)) {
            return;
        }

        try {
            Tenant tenant = tenantManager.getTenant(rUser.getOboTenantId());
            String baseUrl = null;
            if (tenant != null) {
                baseUrl = tenant.getBaseUrl();
                if (!StringUtils.isBlank(baseUrl)) {
                    URI redeemURI = UriBuilder.fromUri(baseUrl).
                            path("v3").
                            path(TapisConstants.SERVICE_NAME_FILES).
                            path("postits/redeem").
                            path(postItDto.getId()).build();
                    postItDto.setRedeemUrl(redeemURI.toString());
                }
            }
        } catch (TapisException ex) {
            // this is not a fatal exception, but we should probably log it so that we can see what's going on.
            String msg = ApiUtils.getMsgAuth("FAPI_POSTITS_OP_ERROR", rUser,
                    opName, postItDto.getSystemId(), postItDto.getPath(), ex.getMessage(), ex.getMessage());
            log.info(msg, ex);
        }
    }
}
