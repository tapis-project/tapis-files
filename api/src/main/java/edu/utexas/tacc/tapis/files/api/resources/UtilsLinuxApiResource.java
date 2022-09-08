package edu.utexas.tacc.tapis.files.api.resources;

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
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
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
import java.time.Duration;
import java.time.Instant;

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
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    try
    {
      Instant start = Instant.now();
      TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
      LibUtils.checkEnabled(user, system);
      String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
      IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);

      // Make the service call
      FileStatInfo fileStatInfo = fileUtilsService.getStatInfo(client, path, followLinks);

      String msg = LibUtils.getMsgAuth("FILES_DURATION", user, opName, systemId, Duration.between(start, Instant.now()).toMillis());
      log.debug(msg);
      TapisResponse<FileStatInfo> resp = TapisResponse.createSuccessResponse("ok", fileStatInfo);
      return Response.status(Status.OK).entity(resp).build();
    }
    catch (ServiceException | IOException e)
    {
      String msg = LibUtils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
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
    AuthenticatedUser user = (AuthenticatedUser) securityContext.getUserPrincipal();
    try
    {
      TapisSystem system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser());
      LibUtils.checkEnabled(user, system);
      String effectiveUserId = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
      IRemoteDataClient client = getClientForUserAndSystem(user, system, effectiveUserId);
      if (client == null) throw new NotFoundException(LibUtils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId));

      // Make the service call
      boolean sharedAppCtxFalse = false;
      NativeLinuxOpResult nativeLinuxOpResult =
              fileUtilsService.linuxOp(client, path, request.getOperation(), request.getArgument(), recursive, sharedAppCtxFalse);

      TapisResponse<NativeLinuxOpResult> resp = TapisResponse.createSuccessResponse("ok", nativeLinuxOpResult);
      return Response.ok(resp).build();
    }
    catch (TapisException | ServiceException | IOException e)
    {
      String msg = LibUtils.getMsgAuth("FILES_OPS_ERR", user, opName, systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }
}
