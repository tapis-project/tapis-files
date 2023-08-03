package edu.utexas.tacc.tapis.files.lib.caches;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import edu.utexas.tacc.tapis.files.lib.transfers.TransfersApp;
import org.jetbrains.annotations.NotNull;
import org.jooq.tools.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * Systems cache. Loads systems without checking auth.
 * Allows service to get system details
 *   Call is made as the Files service, files@<admin-tenant>
 *   Resolved effectiveUserId should never be used, credentials are not retrieved
 * Cache key is: (tenantId, systemId)
 */
@Service
public class SystemsCacheNoAuth
{
  private static final Logger log = LoggerFactory.getLogger(SystemsCacheNoAuth.class);

  // Cache timeout
  private static final long CACHE_TIMEOUT_SECONDS = 60;

  // NotAuthorizedException requires a Challenge
  private static final String NO_CHALLENGE = "NoChallenge";

  private final LoadingCache<SystemCacheKey, TapisSystem> cache;
  private final ServiceClients serviceClients;

  @Inject
  public SystemsCacheNoAuth(ServiceClients svcClients)
  {
    serviceClients = svcClients;
    cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofSeconds(CACHE_TIMEOUT_SECONDS)).build(new SystemLoader());
  }

  public TapisSystem getSystem(String tenantId, String systemId, String tapisUser) throws ServiceException
  {
    try
    {
      SystemCacheKey key = new SystemCacheKey(tenantId, systemId, tapisUser);
      return cache.get(key);
    }
    catch (ExecutionException ex)
    {
      // Get the cause. If it is a TapisClientException we need to figure out what happened. NotFound, Forbidden, etc.
      var cause = ex.getCause();
      String msg;
      if (cause instanceof TapisClientException)
      {
        var tce = (TapisClientException) cause;
        Response.Status status = Response.Status.fromStatusCode(tce.getCode());
        msg = cause.getMessage();
        switch (status)
        {
          case NOT_FOUND ->  throw new NotFoundException(msg);
          case FORBIDDEN ->  throw new ForbiddenException(msg);
          case UNAUTHORIZED -> throw new NotAuthorizedException(msg, NO_CHALLENGE);
          case BAD_REQUEST -> throw new BadRequestException(msg);
          case INTERNAL_SERVER_ERROR -> throw new WebApplicationException(msg);
        }
      }
      // It was something other than a TapisClientException or fromStatusCode returned null or some unhandled code.
      String usernameNull = null;
      msg = LibUtils.getMsg("FILES_CACHE_ERR", "Systems", tenantId, systemId, usernameNull, ex.getMessage());
      throw new WebApplicationException(msg, ex);
    }
  }

  public void invalidateEntry(String tenantId, String systemId, String tapisUser) {
    SystemCacheKey key = new SystemCacheKey(tenantId, systemId, tapisUser);
    cache.invalidate(key);
  }

  // ====================================================================================
  // =======  Private Classes ===========================================================
  // ====================================================================================

  /**
   * Class implementing method needed for populating the cache.
   */
  private class SystemLoader extends CacheLoader<SystemCacheKey, TapisSystem>
  {
    @NotNull
    @Override
    public TapisSystem load(SystemCacheKey key) throws Exception
    {
      log.debug(LibUtils.getMsg("FILES_CACHE_NOAUTH_SYS_LOADING", key.getTenantId(), key.getSystemId(), key.getTapisUser()));
      // Create a client to call systems as files@<admin-tenant>
      // Service admin tenant might be FileOpsService or TransfersApp, depending on whether we are running as part of
      //   the FilesApplication or the TransfersApp.
      String svcTenant = FileOpsService.getServiceTenantId();
      if (StringUtils.isBlank(svcTenant)) svcTenant = TransfersApp.getSiteAdminTenantId();
      SystemsClient client = serviceClients.getClient(TapisConstants.SERVICE_NAME_FILES, svcTenant, SystemsClient.class);
      // Use impersonationId and resourceTenant to pass in the user and tenant for the requested system.
      // Because this is Files calling as itself we must tell Systems which user and tenant to use.
      // Systems normally gets these from the jwt.
      SystemsClient.AuthnMethod authnMethod = null;
      var requireExec = false;
      var selectStr = "allAttributes";
      var returnCreds = true;
      String sharedCtxNull = null;
      String impersonationId = key.getTapisUser();
      String resourceTenant = key.getTenantId();
      TapisSystem system = client.getSystem(key.getSystemId(), authnMethod, requireExec, selectStr, returnCreds,
                                            impersonationId, sharedCtxNull, resourceTenant);
      log.debug(LibUtils.getMsg("FILES_CACHE_NOAUTH_SYS_LOADED", key.getTenantId(), key.getSystemId(), key.getTapisUser()));
      return system;
    }
  }

  /**
   * Class representing the cache key.
   * Unique keys for tenantId+systemId+tapisUser+impersonationId+sharedCtxGrantor
   */
  private static class SystemCacheKey
  {
    private final String tenantId;
    private final String systemId;
    private final String tapisUser;

    public SystemCacheKey(String tenantId1, String systemId1, String tapisUser1)
    {
      systemId = systemId1;
      tenantId = tenantId1;
      tapisUser = tapisUser1;
    }

    // ====================================================================================
    // =======  Accessors =================================================================
    // ====================================================================================
    public String getTenantId() { return tenantId; }
    public String getSystemId() { return systemId; }
    public String getTapisUser() { return tapisUser; }

    // ====================================================================================
    // =======  Support for equals ========================================================
    // ====================================================================================
    @Override
    public boolean equals(Object o)
    {
      if (o == this) return true;
      // Note: no need to check for o==null since instanceof will handle that case
      if (!(o instanceof SystemCacheKey)) return false;
      var that = (SystemCacheKey) o;
      return (Objects.equals(this.tenantId, that.tenantId) && Objects.equals(this.systemId, that.systemId)
              && Objects.equals(this.tapisUser, that.tapisUser));
    }

    @Override
    public int hashCode() { return Objects.hash(tenantId, systemId, tapisUser); }
  }
}
