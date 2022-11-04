package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/*
 * Systems cache. Loads systems with credentials.
 * For use when in a shared app context.
 * Uses sharedAppCtx = true in order to skip Tapis auth.
 *   effectiveUserId is resolved.
 *   Default AuthnMethod is used.
 */
@Service
public class SystemsCacheNoAuth implements ISystemsCache
{
  private static final Logger log = LoggerFactory.getLogger(SystemsCacheNoAuth.class);

  private final LoadingCache<SystemCacheKey, TapisSystem> cache;
  private final ServiceClients serviceClients;

  @Inject
  public SystemsCacheNoAuth(ServiceClients svcClients)
  {
    serviceClients = svcClients;
    IRuntimeConfig config = RuntimeSettings.get();
    cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofMinutes(5)).build(new SystemLoader());
  }


  @Override
  public TapisSystem getSystem(String tenantId, String systemId, String username) throws ServiceException
  {
    try {
      SystemCacheKey key = new SystemCacheKey(tenantId, systemId, username);
      return cache.get(key);
    }
    catch (ExecutionException ex)
    {
      String msg = LibUtils.getMsg("FILES_CACHE_ERR", "Systems", tenantId, systemId, username, ex.getMessage());
      throw new ServiceException(msg, ex);
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
    @Override
    public TapisSystem load(SystemCacheKey key) throws Exception
    {
      log.debug(LibUtils.getMsg("FILES_CACHE_SYS_LOADING", key.getTenantId(), key.getSystemId(), key.getUsername()));
      SystemsClient client = serviceClients.getClient(key.getUsername(), key.getTenantId(), SystemsClient.class);
      // Use sharedAppCtx = true to bypass Tapis auth
      SystemsClient.AuthnMethod authnMethod = null;
      var requireExec = false;
      var selectStr = "allAttributes";
      var returnCreds = true;
      String impersonationId = null;
      var sharedAppCtx = true;
      TapisSystem system = client.getSystem(key.getSystemId(), authnMethod, requireExec, selectStr, returnCreds,
                                            impersonationId, sharedAppCtx);
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