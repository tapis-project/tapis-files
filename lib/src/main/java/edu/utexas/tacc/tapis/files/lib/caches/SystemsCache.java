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
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * Systems cache. Loads systems with credentials.
 * For use when not in a shared app context.
 * Standard Tapis auth check is done.
 *   effectiveUserId is resolved.
 *   Default AuthnMethod is used.
 */
@Service
public class SystemsCache implements ISystemsCache
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
    IRuntimeConfig config = RuntimeSettings.get();
    cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(5)).build(new SystemLoader());
  }


  @Override
  public TapisSystem getSystem(String tenantId, String systemId, String username) throws ServiceException
  {
    try
    {
      SystemCacheKey key = new SystemCacheKey(tenantId, systemId, username);
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
      msg = LibUtils.getMsg("FILES_CACHE_ERR", "Systems", tenantId, systemId, username, ex.getMessage());
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
      log.debug(LibUtils.getMsg("FILES_CACHE_SYS_LOADING", key.getTenantId(), key.getSystemId(), key.getUsername()));
      SystemsClient client = serviceClients.getClient(key.getUsername(), key.getTenantId(), SystemsClient.class);
      TapisSystem system = client.getSystemWithCredentials(key.getSystemId());
      log.debug(LibUtils.getMsg("FILES_CACHE_SYS_LOADED", key.getTenantId(), key.getSystemId(), key.getUsername(),
                                system.getDefaultAuthnMethod()));
      return system;
    }
  }

  /**
   * Class representing the cache key.
   * Unique keys for tenantId+systemId+user
   */
  private static class SystemCacheKey
  {
    private final String tenantId;
    private final String systemId;
    private final String username;

    public SystemCacheKey(String tenantId1, String systemId1, String username1)
    {
      systemId = systemId1;
      tenantId = tenantId1;
      username = username1;
    }

    // ====================================================================================
    // =======  Accessors =================================================================
    // ====================================================================================
    public String getTenantId()
    {
      return tenantId;
    }
    public String getSystemId()
    {
      return systemId;
    }
    public String getUsername()
    {
      return username;
    }

    // ====================================================================================
    // =======  Support for equals ========================================================
    // ====================================================================================
    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SystemCacheKey that = (SystemCacheKey) o;
      if (!tenantId.equals(that.tenantId)) return false;
      if (!systemId.equals(that.systemId)) return false;
      return username.equals(that.username);
    }

    @Override
    public int hashCode() { return Objects.hash(tenantId, systemId, username); }
  }
}
