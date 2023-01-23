package edu.utexas.tacc.tapis.files.api.resources;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import edu.utexas.tacc.tapis.files.api.models.NativeLinuxFaclRequest;
import edu.utexas.tacc.tapis.files.lib.models.AclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.api.models.NativeLinuxOpRequest;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileStatInfo;
import edu.utexas.tacc.tapis.files.lib.models.NativeLinuxOpResult;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * JAX-RS REST resource for Tapis file native linux operations (stat, chmod, chown, chgrp)
 * jax-rs annotations map HTTP verb + endpoint to method invocation and map query parameters.
 *  NOTE: For OpenAPI spec please see repo openapi-files, file FilesAPI.yaml
 *
 * Another option would be to have systemType as a path parameter and rename this class to UtilsApiResource.
 */
@Path("/v3/files/utils/linux")
public class UtilsLinuxApiResource extends BaseFileOpsResource
{
  private static final Logger log = LoggerFactory.getLogger(UtilsLinuxApiResource.class);

  @Inject
  FileUtilsService fileUtilsService;

  @GET
  @Path("/{systemId}/{path:(.*+)}") // Path is optional here, have to do this regex madness.
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatInfo(@PathParam("systemId") String systemId,
                              @PathParam("path") String path,
                              @QueryParam("followLinks") @DefaultValue("false") boolean followLinks,
                              @Context SecurityContext securityContext)
  {
    String opName = "getStatInfo";
    // Create a user that collects together tenant, user and request information needed by the service call
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    Instant start = Instant.now();
    FileStatInfo fileStatInfo;
    try
    {
      // NOTE: Allow for linux operations on systems that are disabled. So do not use getSystemIfEnabled.
      TapisSystem system = systemsCache.getSystem(oboTenant, systemId, oboUser);
      LibUtils.checkEnabled(rUser, system);
      IRemoteDataClient client = getClientForUserAndSystem(rUser, system);

      // Make the service call
      fileStatInfo = fileUtilsService.getStatInfo(client, path, followLinks);
    }
    catch (ServiceException | IOException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPS_ERR", rUser, opName, systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = LibUtils.getMsgAuthR("FILES_DURATION", rUser, opName, systemId, Duration.between(start, Instant.now()).toMillis());
    log.debug(msg);
    msg = LibUtils.getMsgAuthR("FILES_DURATION", rUser, opName, systemId, Duration.between(start, Instant.now()).toMillis());
    TapisResponse<FileStatInfo> resp = TapisResponse.createSuccessResponse(msg, fileStatInfo);
    return Response.ok(resp).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{systemId}/{path:.+}")
  public Response runLinuxNativeOp(@PathParam("systemId") String systemId,
                                   @PathParam("path") String path,
                                   @Valid NativeLinuxOpRequest request,
                                   @QueryParam("recursive") @DefaultValue("false") boolean recursive,
                                   @Context SecurityContext securityContext)
  {
    String opName = "runLinuxNativeOp";
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    String oboUser = rUser.getOboUserId();
    // Make sure the Tapis System exists and is enabled
    TapisSystem system = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    NativeLinuxOpResult nativeLinuxOpResult;
    try
    {
      IRemoteDataClient client = getClientForUserAndSystem(rUser, system);

      // Make the service call
      nativeLinuxOpResult = fileUtilsService.linuxOp(client, path, request.getOperation(), request.getArgument(), recursive);
    }
    catch (TapisException | ServiceException | IOException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPS_ERR", rUser, opName, systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    String msg = ApiUtils.getMsgAuth("FAPI_LINUX_OP_DONE", rUser, request.getOperation().name(), systemId, path);
    TapisResponse<NativeLinuxOpResult> resp = TapisResponse.createSuccessResponse(msg, nativeLinuxOpResult);
    return Response.ok(resp).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("facl/{systemId}/{path:.+}")
  public Response runLinuxGetfacl(@PathParam("systemId") String systemId,
                                   @PathParam("path") String path,
                                   @Context SecurityContext securityContext)
  {
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    String oboUser = rUser.getOboUserId();

    // Make sure the Tapis System exists and is enabled
    TapisSystem system = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // check system owner is obouser

    List<AclEntry> aclEntries;
    try
    {
      IRemoteDataClient client = getClientForUserAndSystem(rUser, system);

      // Make the service call
      aclEntries = fileUtilsService.getfacl(client, path);
    }
    catch (TapisException | ServiceException | IOException e)
    {
      // TODO fix error
      String msg = LibUtils.getMsgAuthR("FILES_OPS_ERR", rUser, "getfacl", systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    // TODO fix messages
    String msg = ApiUtils.getMsgAuth("FAPI_LINUX_OP_DONE", rUser, "getfacl", systemId, path);
    TapisResponse<List<AclEntry>> resp = TapisResponse.createSuccessResponse(msg, aclEntries);
    return Response.ok(resp).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("facl/{systemId}/{path:.+}")
  public Response runLinuxSetfacl(@PathParam("systemId") String systemId,
                                  @PathParam("path") String path,
                                  @Valid NativeLinuxFaclRequest request,
                                  @Context SecurityContext securityContext)
  {
    // Create a user that collects together tenant, user and request information needed by service calls
    ResourceRequestUser rUser = new ResourceRequestUser((AuthenticatedUser) securityContext.getUserPrincipal());
    String oboUser = rUser.getOboUserId();

    // Make sure the Tapis System exists and is enabled
    TapisSystem system = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // check system owner is obouser

    NativeLinuxOpResult nativeLinuxOpResult;
    try
    {
      IRemoteDataClient client = getClientForUserAndSystem(rUser, system);

      // Make the service call
      nativeLinuxOpResult = fileUtilsService.setfacl(client, path, request.getOperation(),
              request.getRecursionMethod(), request.getAclString());
    }
    catch (TapisException | ServiceException | IOException e)
    {
      // TODO fix error
      String msg = LibUtils.getMsgAuthR("FILES_OPS_ERR", rUser, "setfacl", systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    // TODO fix messages
    String msg = ApiUtils.getMsgAuth("FAPI_LINUX_OP_DONE", rUser, "setfacl", systemId, path);
    TapisResponse<NativeLinuxOpResult> resp = TapisResponse.createSuccessResponse(msg, nativeLinuxOpResult);
    return Response.ok(resp).build();
  }

}
