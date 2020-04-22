package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SSHConnectionCache {

    private static final Logger log = LoggerFactory.getLogger(SSHConnectionCache.class);

    private static final LoadingCache<SSHConnectionCacheKey, SSHConnection> sessionCache = CacheBuilder.newBuilder()
            .expireAfterAccess(60, TimeUnit.SECONDS)
            .removalListener(new SSHConnectionCacheRemover())
            .build(new SSHConnectionCacheLoader());

    public static SSHConnection getConnection(TSystem system, String username) throws IOException {
        SSHConnectionCacheKey key = new SSHConnectionCacheKey(system, username);
        try {
            return sessionCache.get(key);
        } catch (ExecutionException ex) {
            log.error("SSHSessionCache get error", ex);
            throw new IOException("Could not get or create SSH session");
        }
    }

}
