package edu.utexas.tacc.tapis.files.api.resources;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.security.PermitAll;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.api.FilesApplication;
import edu.utexas.tacc.tapis.files.api.utils.ApiUtils;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.utils.CallSiteToggle;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.responses.RespBasic;
import static edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils.RESPONSE_STATUS;

/*
 * Tapis Files general resource endpoints including healthcheck and readycheck
 *
 *  NOTE: For OpenAPI spec please see file FilesApi.yaml located in repo openapi-files.
 */
@Path("/v3/files")
public class GeneralResource
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(GeneralResource.class);

  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // Count the number of health check requests received.
  private static final AtomicLong _healthCheckCount = new AtomicLong();

  // Count the number of health check requests received.
  private static final AtomicLong _readyCheckCount = new AtomicLong();

  // Use CallSiteToggle to limit logging for readyCheck endpoint
  private static final CallSiteToggle checkTenantsOK = new CallSiteToggle();
  private static final CallSiteToggle checkJWTOK = new CallSiteToggle();
  private static final CallSiteToggle checkDBOK = new CallSiteToggle();
  private static final CallSiteToggle checkMQOK = new CallSiteToggle();

  // **************** Inject Services using HK2 ****************
  @Inject
  private ServiceContext serviceContext;

  @Inject
  private TransfersService transfersService;

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Lightweight non-authenticated health check endpoint.
   * Note that no JWT is required on this call and no logging is done.
   * @return a success response if all is ok
   */
  @GET
  @Path("/healthcheck")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response healthCheck()
  {
    // Get the current check count.
    long checkNum = _healthCheckCount.incrementAndGet();
    RespBasic resp = new RespBasic("Health check received. Count: " + checkNum);

    // Manually create a success response with git info included in version
    resp.status = RESPONSE_STATUS.success.name();
    resp.message = MsgUtils.getMsg("TAPIS_HEALTHY", "Files Service");
    resp.version = TapisUtils.getTapisFullVersion();
    resp.commit = TapisUtils.getGitCommit();
    resp.build = TapisUtils.getBuildTime();
    return Response.ok(resp).build();
  }

  /**
   * Lightweight non-authenticated ready check endpoint.
   * Note that no JWT is required on this call and CallSiteToggle is used to limit logging.
   * Based on similar method in tapis-securityapi.../SecurityResource
   *
   * For this service readiness means service can:
   *    - retrieve tenants map
   *    - get a service JWT
   *    - TODO: connect to the DB and verify and that main service table exists
   *
   * It's intended as the endpoint that monitoring applications can use to check
   * whether the service is ready to accept traffic.  In particular, kubernetes
   * can use this endpoint as part of its pod readiness check.
   *
   * Note that no JWT is required on this call.
   *
   * A good synopsis of the difference between liveness and readiness checks:
   *
   * ---------
   * The probes have different meaning with different results:
   *
   *    - failing liveness probes  -> restart pod
   *    - failing readiness probes -> do not send traffic to that pod
   *
   * See https://stackoverflow.com/questions/54744943/why-both-liveness-is-needed-with-readiness
   * ---------
   *
   * @return a success response if all is ok
   */
  @GET
  @Path("/readycheck")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PermitAll
  public Response readyCheck()
  {
    // Get the current check count.
    long checkNum = _readyCheckCount.incrementAndGet();

    // Check that we can get tenants list
    Exception readyCheckException = checkTenants();
    if (readyCheckException != null)
    {
      RespBasic r = new RespBasic("Readiness tenants check failed. Check number: " + checkNum);
      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Files Service");
      // We failed so set the log limiter check.
      if (checkTenantsOK.toggleOff())
      {
        _log.warn(msg, readyCheckException);
        _log.warn(ApiUtils.getMsg("FILES_READYCHECK_TENANTS_ERRTOGGLE_SET"));
      }
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
    }
    else
    {
      // We succeeded so clear the log limiter check.
      if (checkTenantsOK.toggleOn()) _log.info(ApiUtils.getMsg("FILES_READYCHECK_TENANTS_ERRTOGGLE_CLEARED"));
    }

    // Check that we have a service JWT
    readyCheckException = checkJWT();
    if (readyCheckException != null)
    {
      RespBasic r = new RespBasic("Readiness JWT check failed. Check number: " + checkNum);
      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Files Service");
      // We failed so set the log limiter check.
      if (checkJWTOK.toggleOff())
      {
        _log.warn(msg, readyCheckException);
        _log.warn(ApiUtils.getMsg("FILES_READYCHECK_JWT_ERRTOGGLE_SET"));
      }
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
    }
    else
    {
      // We succeeded so clear the log limiter check.
      if (checkJWTOK.toggleOn()) _log.info(ApiUtils.getMsg("FILES_READYCHECK_JWT_ERRTOGGLE_CLEARED"));
    }

    // Check that we can connect to the DB
    if (!transfersService.isConnectionOk(Duration.ofSeconds(5), 1)) {
      RespBasic r = new RespBasic("Readiness message queue check failed. Check number: " + checkNum);
      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Files Service");
      // We failed so set the log limiter check.
      if (checkMQOK.toggleOff())
      {
        _log.warn(msg, readyCheckException);
        _log.warn(LibUtils.getMsg("FILES_READYCHECK_MQ_ERRTOGGLE_SET"));
      }
      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
    }
    else
    {
      // We succeeded so clear the log limiter check.
      if (checkMQOK.toggleOn()) _log.info(LibUtils.getMsg("FILES_READYCHECK_MQ_ERRTOGGLE_CLEARED"));
    }

//    // Check that we can connect to the DB
//    readyCheckException = checkDB();
//    if (readyCheckException != null)
//    {
//      RespBasic r = new RespBasic("Readiness DB check failed. Check number: " + checkNum);
//      String msg = MsgUtils.getMsg("TAPIS_NOT_READY", "Files Service");
//      // We failed so set the log limiter check.
//      if (checkDBOK.toggleOff())
//      {
//        _log.warn(msg, readyCheckException);
//        _log.warn(ApiUtils.getMsg("FILES_READYCHECK_DB_ERRTOGGLE_SET"));
//      }
//      return Response.status(Status.SERVICE_UNAVAILABLE).entity(TapisRestUtils.createErrorResponse(msg, false, r)).build();
//    }
//    else
//    {
//      // We succeeded so clear the log limiter check.
//      if (checkDBOK.toggleOn()) _log.info(ApiUtils.getMsg("FILES_READYCHECK_DB_ERRTOGGLE_CLEARED"));
//    }

    // ---------------------------- Success -------------------------------
    // Create the response payload.
    RespBasic resp = new RespBasic("Ready check passed. Count: " + checkNum);
    // Manually create a success response with git info included in version
    resp.status = RESPONSE_STATUS.success.name();
    resp.message = MsgUtils.getMsg("TAPIS_READY", "Files Service");
    resp.version = TapisUtils.getTapisFullVersion();
    resp.commit = TapisUtils.getGitCommit();
    resp.build = TapisUtils.getBuildTime();
    return Response.ok(resp).build();
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Verify that we have a valid service JWT.
   * @return null if OK, otherwise return an exception
   */
  private Exception checkJWT()
  {
    Exception result = null;
    try
    {
      // Make sure we have one.
      String jwt = serviceContext.getServiceJWT().getAccessJWT(FilesApplication.getSiteId());
      if (StringUtils.isBlank(jwt))
      {
        result = new TapisClientException(LibUtils.getMsg("FILES_CHECKJWT_EMPTY"));
      }
      // Make sure it has not expired
      if (serviceContext.getServiceJWT().hasExpiredAccessJWT(FilesApplication.getSiteId()))
      {
        result =  new TapisClientException(LibUtils.getMsg("FILES_CHECKJWT_EXPIRED"));
      }
    }
    catch (Exception e) { result = e; }
    return result;
  }

//  /**
//   * Check the database
//   * @return null if OK, otherwise return an exception
//   */
//  private Exception checkDB()
//  {
//    Exception result;
//    try { result = svcImpl.checkDB(); }
//    catch (Exception e) { result = e; }
//    return result;
//  }
//
  /**
   * Retrieve the cached tenants map.
   * @return null if OK, otherwise return an exception
   */
  private Exception checkTenants()
  {
    Exception result = null;
    try
    {
      // Make sure the cached tenants map is not null or empty.
      var tenantMap = TenantManager.getInstance().getTenants();
      if (tenantMap == null || tenantMap.isEmpty()) result = new TapisClientException(LibUtils.getMsg("FILES_CHECKTENANTS_EMPTY"));
    }
    catch (Exception e) { result = e; }
    return result;
  }
}
