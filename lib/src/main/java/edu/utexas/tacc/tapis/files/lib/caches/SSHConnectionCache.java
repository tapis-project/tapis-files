package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
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
 *
 * https://github.com/google/guava/wiki/CachesExplained
 *
 * NOTE: If performance or reliability becomes an issue may want to consider using Caffeine or Ehcache.
 *       Caffeine is the successor to Google's guava cache and is supposed to be close to a drop in replacement.
 */
public class SSHConnectionCache
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);
  private static LoadingCache<SSHConnectionCacheKey, SSHConnectionHolder> sessionCache;

  /**
   * Constructor for the cache
   * NOTE: Can add recordStats() to the CacheBuilder if we want to monitor stats
   * @param maxSize maximum size
   * @param timeout Timeout of when to preform maintenance
   * @param timeUnit TimeUnit of timeout
   */
  public SSHConnectionCache(long maxSize, long timeout, TimeUnit timeUnit)
  {
    // Create a cache that uses SSHConnectionCacheLoader to create new entries.
    // Set max size and have the cache expire entries using timeout values
    // Note that we use expireAfterWrite instead of expireAfterAccess because of the concern that a user's
    //   credentials will change. If we used expireAfterAccess and a system+user is accessed frequently we might
    //   never pick up the new credentials.
    sessionCache = CacheBuilder.newBuilder().maximumSize(maxSize).expireAfterWrite(timeout, timeUnit)
                               .removalListener(new SSHConnectionStop())
                               .build(new SSHConnectionCacheLoader());
    // Start a thread to signal the cache to clean up. The cache itself does not have a thread. Instead, it performs
    //   small amounts of maintenance during read/write.
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(() -> sessionCache.cleanUp(), timeout, timeout, timeUnit);
//    executorService.scheduleAtFixedRate(
//            ()-> sessionCache.asMap().forEach((SSHConnectionCacheKey key, SSHConnectionHolder holder) ->
//            {
//
//              // TODO/TBD: Move closing to a RemovalListener
//              if (holder.getChannelCount() == 0)
//              {
//                log.debug("Closing SSH connecting from cache: {} for user {}", holder.getSshConnection().getHost(),
//                          holder.getSshConnection().getUsername());
//                sessionCache.invalidate(key);
//        // TODO/TBD: See github issue https://github.com/tapis-project/tapis-files/issues/39
//        //  seems that there may be a race condition. The stop sets the session to null but the
//        //  cached connection is still available? See SSHConnectionHolder
//                holder.getSshConnection().stop();
//              }
//            }
//            ), timeout, timeout, timeUnit);
  }

  /**
   * Get a connection from the cache.
   * @param system System object
   * @param oboUser API username
   * @return an SSH connection.
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
