package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;


/**
 * The SSHConnectionCache stores SSH connections in a Guava LoadingCache which is periodically
 * checked to see if the SSH sessions are active or not. If the session is not active, it
 * is disconnected and removed from the cache. THe next get() operation on the cache will reinstantiate
 * the connection and place it back in the cache.
 *
 * A single threaded ScheduledExecutorService is used to periodically do the maintenance
 *
 * The cache key is a combination of systemId + username
 */
public class SSHConnectionCache {

    private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);
    private static LoadingCache<SSHConnectionCacheKey, SSHConnection> sessionCache;

    /**
     *
     * @param timeout Timeout in seconds of when to preform maintenence
     */
    public SSHConnectionCache(long timeout, TimeUnit timeUnit) {
        sessionCache = CacheBuilder.newBuilder()
                .recordStats()
                .build(new SSHConnectionCacheLoader());
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay( ()-> {
            log.info("Refreshing the cache");
            sessionCache.asMap().forEach( (SSHConnectionCacheKey key, SSHConnection connection) -> {
                if (connection.getChannelCount() == 0) {
                    connection.closeSession();
                    sessionCache.invalidate(key);
                }
            });
        }, 0,timeout, timeUnit);
    }

    public CacheStats getCacheStats() {
        return sessionCache.stats();
    }

    public LoadingCache<SSHConnectionCacheKey, SSHConnection> getCache() {
        return sessionCache;
    }

    public SSHConnection getConnection(TSystem system, String username) throws IOException {
        SSHConnectionCacheKey key = new SSHConnectionCacheKey(system, username);
        try {
            return sessionCache.get(key);
        } catch (ExecutionException ex) {
            log.error("SSHSessionCache get error", ex);
            throw new IOException("Could not get or create SSH session");
        }
    }

}
