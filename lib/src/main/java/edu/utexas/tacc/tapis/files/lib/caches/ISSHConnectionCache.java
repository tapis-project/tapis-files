package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

import java.io.IOException;

public interface ISSHConnectionCache {

    CacheStats  getCacheStats();
    LoadingCache<SSHConnectionCacheKey, SSHConnection> getCache();
    SSHConnection getConnection(TapisSystem system, String username) throws IOException;
}
