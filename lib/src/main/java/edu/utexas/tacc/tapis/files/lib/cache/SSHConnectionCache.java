package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Service
public class SSHConnectionCache {

    private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);
    private static LoadingCache<SSHConnectionCacheKey, SSHConnection> sessionCache;
    private Duration cacheDuration;

    public LoadingCache<SSHConnectionCacheKey, SSHConnection> getCache() {
        return sessionCache;
    }

    public static SSHConnection getConnection(TSystem system, String username) throws IOException {
        SSHConnectionCacheKey key = new SSHConnectionCacheKey(system, username);
        try {
            return sessionCache.get(key);
        } catch (ExecutionException ex) {
            log.error("SSHSessionCache get error", ex);
            throw new IOException("Could not get or create SSH session");
        }
    }

    public Duration getCacheDuration() {
        return cacheDuration;
    }

}
