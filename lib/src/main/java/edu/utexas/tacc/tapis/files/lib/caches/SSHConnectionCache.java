package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
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
 * is disconnected and removed from the cache. THe next get() operation on the cache will reinstantiate
 * the connection and place it back in the cache.
 **
 * A single threaded ScheduledExecutorService is used to periodically do the maintenance
 *
 * The cache key is a combination of systemId, tenant and username
 */
public class SSHConnectionCache {

    private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);
    private static LoadingCache<SSHConnectionCacheKey, SSHConnectionHolder> sessionCache;
    private  final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final TimeUnit timeUnit;
    private final long timeout;
    /**
     *
     * @param timeout Timeout of when to preform maintenance
     * @param timeUnit TimeUnit of timeout
     */
    public SSHConnectionCache(long timeout, TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        this.timeout = timeout;
        sessionCache = CacheBuilder.newBuilder()
                .recordStats()
                .build(new SSHConnectionCacheLoader());
        executorService.scheduleAtFixedRate( ()-> {
            sessionCache.asMap().forEach( (SSHConnectionCacheKey key, SSHConnectionHolder holder) -> {
                //TODO: Should never go negative!
                if (holder.getChannelCount() == 0) {
                    log.info("Closing SSH connecting from cache: {} for user {}", holder.getSshConnection().getHost(), holder.getSshConnection().getUsername());
                    //Not sure what to do there?
                    holder.getSshConnection().stop();
                    sessionCache.invalidate(key);
                }
            });
        }, this.timeout, this.timeout, this.timeUnit); //
    }


    /**
     *
     * @return stats for the cache
     */
    public CacheStats getCacheStats() {
        return sessionCache.stats();
    }

    /**
     *
     * @return
     */
    public LoadingCache<SSHConnectionCacheKey, SSHConnectionHolder> getCache() {
        return sessionCache;
    }


    /**
     *
     * @param system System object
     * @param username API username
     * @return will return a SSH connection, either directly from the cache (if it exists), or
     * create one and return it from the cache.
     * @throws IOException
     */
    public SSHConnectionHolder getConnection(TapisSystem system, String username) throws IOException {
        SSHConnectionCacheKey key = new SSHConnectionCacheKey(system, username);
        try {
            return sessionCache.get(key);
        } catch (ExecutionException ex) {
            log.error("SSHSessionCache get error", ex);
            throw new IOException("Could not get or create SSH session");
        }
    }

}
