package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The SSHConnectionCache stores SSH connections in a Guava LoadingCache which is periodically
 * checked to see if the SSH sessions are active or not. If the session is not active, it
 * is disconnected and removed from the cache. THe next get() operation on the cache will create a new
 * connection and place it back in the cache.
 **
 * A single threaded ScheduledExecutorService is used to periodically do the maintenance
 *
 * The cache key is a combination of systemId, tenant and username
 */
public class SSHConnectionCache
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);
  private static LoadingCache<SSHConnectionCacheKey, SSHConnectionHolder> sessionCache;

  /**
   * Constructor for the cache
   * @param timeout Timeout of when to preform maintenance
   * @param timeUnit TimeUnit of timeout
   */
  public SSHConnectionCache(long timeout, TimeUnit timeUnit)
  {
    sessionCache = CacheBuilder.newBuilder().recordStats().build(new SSHConnectionCacheLoader());
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate( ()-> {
      sessionCache.asMap().forEach( (SSHConnectionCacheKey key, SSHConnectionHolder holder) -> {
        if (holder.getChannelCount() == 0) {
          log.debug("Closing SSH connecting from cache: {} for user {}", holder.getSshConnection().getHost(), holder.getSshConnection().getUsername());
          sessionCache.invalidate(key);
          holder.getSshConnection().stop();
        }
      });
    }, timeout, timeout, timeUnit);
  }

  /**
   * Return the cache stats
   * @return stats for the cache
   */
  public CacheStats getCacheStats() {
    return sessionCache.stats();
  }

  /**
   * Return the internal google cache
   * @return the internal google cache
   */
  public LoadingCache<SSHConnectionCacheKey, SSHConnectionHolder> getCache() {
    return sessionCache;
  }

  /**
   * Get a connection from the cache.
   * @param system System object
   * @param oboUser API username
   * @return an SSH connection, either from the cache (if it exists), or create one and return it from the cache.
   * @throws IOException on error
   */
  public SSHConnectionHolder getConnection(TapisSystem system, String oboUser) throws IOException
  {
    SSHConnectionCacheKey key = new SSHConnectionCacheKey(system, oboUser);
    try { return sessionCache.get(key); }
    catch (ExecutionException ex)
    {
      String msg = Utils.getMsg("FILES_CLIENT_SSH_CONN_ERR", system.getTenant(), oboUser, system.getId(), oboUser,
              system.getHost(), ex.getMessage());
      log.error(msg, ex);
      throw new IOException("Could not get or create SSH session");
    }
  }
}
