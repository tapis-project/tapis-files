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
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * Systems cache. Loads systems with credentials.
 *   Standard Tapis auth check for READ is done.
 *   effectiveUserId is resolved.
 *   Default AuthnMethod is used.
 */
@Service
public class SystemsCache
{
  private static final Logger log = LoggerFactory.getLogger(SystemsCache.class);

  // NotAuthorizedException requires a Challenge
  private static final String NO_CHALLENGE = "NoChallenge";

  private final LoadingCache<SystemCacheKey, TapisSystem> cache;
  private final ServiceClients serviceClients;

  @Inject
  public SystemsCache(ServiceClients svcClients)
  {
    serviceClients = svcClients;
    cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(5)).build(new SystemLoader());
  }

  public TapisSystem getSystem(String tenantId, String systemId, String tapisUser) throws ServiceException
  {
    return getSystem(tenantId, systemId, tapisUser, null, null);
  }

  public TapisSystem getSystem(String tenantId, String systemId, String tapisUser,
                               String impersonationId, String sharedCtxGrantor)
          throws ServiceException
  {
    try
    {
      SystemCacheKey key = new SystemCacheKey(tenantId, systemId, tapisUser, impersonationId, sharedCtxGrantor);
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
      msg = LibUtils.getMsg("FILES_CACHE_ERR", "Systems", tenantId, systemId, tapisUser, ex.getMessage());
      throw new WebApplicationException(msg, ex);
    }
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
      log.debug(LibUtils.getMsg("FILES_CACHE_SYS_LOADING", key.getTenantId(), key.getSystemId(), key.getTapisUser(),
                                 key.getImpersonationId(), key.getSharedCtxGrantor()));
      SystemsClient client = serviceClients.getClient(key.getTapisUser(), key.getTenantId(), SystemsClient.class);

      SystemsClient.AuthnMethod authnMethod = null;
      var requireExec = false;
      var selectStr = "allAttributes";
      var returnCreds = true;
      TapisSystem system = client.getSystem(key.getSystemId(), authnMethod, requireExec, selectStr, returnCreds,
                                            key.getImpersonationId(), key.getSharedCtxGrantor());
      // If client returns null it means not found
      if (system == null)
      {
        String msg = LibUtils.getMsg("FILES_CACHE_SYS_NULL", key.getTenantId(), key.getSystemId(), key.getTapisUser(),
                                     key.getImpersonationId(), key.getSharedCtxGrantor());
        throw new NotFoundException(msg);
      }
      log.debug(LibUtils.getMsg("FILES_CACHE_SYS_LOADED", key.getTenantId(), key.getSystemId(), key.getTapisUser(),
                                key.getImpersonationId(), key.getSharedCtxGrantor(), system.getDefaultAuthnMethod()));
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
    private final String impersonationId;
    private final String sharedCtxGrantor;

    public SystemCacheKey(String tenantId1, String systemId1, String tapisUser1, String impersonationId1, String sharedCtxGrantor1)
    {
      systemId = systemId1;
      tenantId = tenantId1;
      tapisUser = tapisUser1;
      impersonationId = impersonationId1;
      sharedCtxGrantor = sharedCtxGrantor1;
    }

    // ====================================================================================
    // =======  Accessors =================================================================
    // ====================================================================================
    public String getTenantId() { return tenantId; }
    public String getSystemId() { return systemId; }
    public String getTapisUser() { return tapisUser; }
    public String getImpersonationId() { return impersonationId; }
    public String getSharedCtxGrantor() { return sharedCtxGrantor; }

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
              && Objects.equals(this.tapisUser, that.tapisUser)
              && Objects.equals(this.impersonationId, that.impersonationId)
              && Objects.equals(this.sharedCtxGrantor, that.sharedCtxGrantor));
    }

    @Override
    public int hashCode() { return Objects.hash(tenantId, systemId, tapisUser, impersonationId, sharedCtxGrantor); }
  }
}
