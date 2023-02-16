package edu.utexas.tacc.tapis.files.lib.caches;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class TenantAdminCache extends BasePermsCache {
    private final LoadingCache<TenantAdminCache.TenantAdminCacheKey, Boolean> cache;
    private static final Logger log = LoggerFactory.getLogger(FilePermsCache.class);

    public TenantAdminCache() {
        cache = CacheBuilder.newBuilder().expireAfterWrite(Duration.ofSeconds(60)).build(new TenantAdminLoader());
    }

    public Boolean checkPerm(String tenantId, String username)
            throws ServiceException
    {
        try {
            TenantAdminCacheKey key = new TenantAdminCacheKey(tenantId, username);
            return cache.get(key);
        } catch (ExecutionException ex) {
            // TODO:  Wrong message - correct this
            String msg = LibUtils.getMsg("FILES_CACHE_ERR", "FilePerms", tenantId, username, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    /**
     * Class implementing method needed for populating the cache.
     */
    private class TenantAdminLoader extends CacheLoader<TenantAdminCacheKey, Boolean>
    {
        @Override
        public Boolean load(TenantAdminCacheKey key) throws Exception
        {
            log.debug(LibUtils.getMsg("TENANT_ADMIN_CACHE_LOADING", key.getTenantId(), key.getUsername()));
            boolean isAdmin = getSKClient().isAdmin(key.getTenantId(), key.getUsername());
            log.debug(LibUtils.getMsg("TENANT_ADMIN_CACHE_LOADED", key.getTenantId(), key.getUsername()));
            return isAdmin;
        }
    }

    private static class TenantAdminCacheKey {
        private final String tenantId;
        private final String username;

        public TenantAdminCacheKey(String tenantId, String username) {
            this.tenantId = tenantId;
            this.username = username;
        }

        // ====================================================================================
        // =======  Accessors =================================================================
        // ====================================================================================
        public String getTenantId() {
            return tenantId;
        }

        public String getUsername() {
            return username;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TenantAdminCacheKey that = (TenantAdminCacheKey) o;
            return Objects.equals(tenantId, that.tenantId) && Objects.equals(username, that.username);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tenantId, username);
        }
    }
}
