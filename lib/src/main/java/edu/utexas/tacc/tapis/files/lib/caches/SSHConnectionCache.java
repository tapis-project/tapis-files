package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The SSHConnectionCache stores SSH connections in a Guava LoadingCache.
 * Entries are periodically marked as stale and removed from the cache.
 *
 * A single threaded ScheduledExecutorService is used to periodically do the maintenance
 *
 * The cache value is SSHConnectionHolder which manages the underlying edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection
 * The cache key is SSHConnectionCacheKey, a combination of tenantId, systemId and effectiveUserId
 *
 * https://github.com/google/guava/wiki/CachesExplained
 *
 * NOTE: If performance or reliability becomes an issue may want to consider using Caffeine or Ehcache.
 *       Caffeine is the successor to Google's guava cache and is supposed to be close to a drop-in replacement.
 */
public class SSHConnectionCache
{
  private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);
  private static LoadingCache<SSHConnectionCacheKey, SSHConnectionHolder> sessionCache;

  /**
   * Constructor for the cache
   * NOTE: Can add recordStats() to the CacheBuilder if we want to monitor stats
   * NOTE: Can have a maxSize using CacheBuilder.newBuilder().maximumSize(maxSize) ...
   * @param timeout Timeout of when to preform maintenance
   * @param timeUnit TimeUnit of timeout
   */
  public SSHConnectionCache(long timeout, TimeUnit timeUnit)
  {
    // Create a cache that uses SSHConnectionCacheLoader to create new entries.
    // Have the cache expire entries using timeout values
    // Note that we use expireAfterWrite instead of expireAfterAccess because of the concern that a user's
    //   credentials will change. If we used expireAfterAccess and a system+user is accessed frequently we might
    //   never pick up the new credentials.
    sessionCache = CacheBuilder.newBuilder().expireAfterWrite(timeout, timeUnit)
            .removalListener(new SSHConnectionExpire())
            .build(new SSHConnectionCacheLoader());
    // Start a thread to signal the cache to clean up. The cache itself does not have a thread. Instead, it performs
    //   small amounts of maintenance during read/write. By starting a thread we can make sure it happens regularly.
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(() -> sessionCache.cleanUp(), timeout, timeout, timeUnit);
  }

  /**
   * Get a connection from the cache.
   * @param system System object
   * @param effUserId API username
   * @return an SSH connection.
   * @throws IOException on error
   */
  public SSHConnectionHolder getConnection(TapisSystem system, String effUserId) throws IOException
  {
    SSHConnectionCacheKey key = new SSHConnectionCacheKey(system, effUserId);
    try { return sessionCache.get(key); }
    catch (ExecutionException ex)
    {
      String msg = LibUtils.getMsg("FILES_CLIENT_SSH_CONN_ERR", system.getTenant(), effUserId, system.getId(),
                                   effUserId, system.getHost(), ex.getMessage());
      log.error(msg, ex);
      throw new IOException(msg, ex);
    }
  }
}
