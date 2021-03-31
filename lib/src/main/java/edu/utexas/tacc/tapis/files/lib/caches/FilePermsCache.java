package edu.utexas.tacc.tapis.files.lib.caches;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import java.time.Duration;
import java.util.concurrent.ExecutionException;

@Service
public class FilePermsCache {


    private final SKClient skClient;
    private final ServiceJWT serviceJWT;
    private final TenantManager tenantCache;
    private final IRuntimeConfig config;
    private final LoadingCache<FilePermCacheKey, Boolean> cache;
    // PERMSPEC is "files:tenant:r,rw,*:systemId:path
    private static final String PERMSPEC = "files:%s:%s:%s:%s";

    @Inject
    public FilePermsCache(SKClient skClient, ServiceJWT serviceJWT, TenantManager tenantCache) {
        this.skClient = skClient;
        this.serviceJWT = serviceJWT;
        this.tenantCache = tenantCache;
        this.config = RuntimeSettings.get();
        cache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .build(new PermsLoader());
    }


    public boolean checkPerm(String tenantId, String username, String systemId, String path, Permission perm) throws ServiceException {
        try {
            FilePermCacheKey key = new FilePermCacheKey(tenantId, systemId, path, username, perm);
            return cache.get(key);
        } catch (ExecutionException ex) {
            String msg = Utils.getMsg("FILES_CACHE_ERR", "FilePerms", tenantId, systemId, username, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

  /**
   * Return Permission or null if no permission granted
   * @param tenantId
   * @param username
   * @param systemId
   * @param path
   * @return Permission - null if no permission granted
   * @throws ServiceException
   */
    public Permission fetchPerm(String tenantId, String username, String systemId, String path) throws ServiceException {
        try {
          FilePermCacheKey key = new FilePermCacheKey(tenantId, systemId, path, username, Permission.READ);
          if (cache.get(key)) return Permission.READ;
          key = new FilePermCacheKey(tenantId, systemId, path, username, Permission.MODIFY);
          if (cache.get(key)) return Permission.MODIFY;
          return null;
        } catch (ExecutionException ex) {
          String msg = Utils.getMsg("FILES_CACHE_ERR", "FilePerms", tenantId, systemId, username, ex.getMessage());
          throw new ServiceException(msg, ex);
        }
  }

  private class PermsLoader extends CacheLoader<FilePermCacheKey, Boolean> {

        @Override
        public Boolean load(FilePermCacheKey key) throws Exception {
            Tenant tenant = tenantCache.getTenant(key.getTenantId());
            skClient.setUserAgent("filesServiceClient");
            skClient.setBasePath(tenant.getBaseUrl() + "/v3");
            skClient.addDefaultHeader("x-tapis-token", serviceJWT.getAccessJWT(config.getSiteId()));
            skClient.addDefaultHeader("x-tapis-user", key.getUsername());
            skClient.addDefaultHeader("x-tapis-tenant", key.getTenantId());
            String permSpec = String.format(PERMSPEC, key.getTenantId(), key.getPerm(), key.getSystemId(), key.getPath());
            return skClient.isPermitted(key.getTenantId(), key.getUsername(), permSpec);
        }
    }

    private static class FilePermCacheKey
    {
        private final String tenantId;
        private final String username;
        private final String systemId;
        private final String path;
        private final Permission perm;

        public FilePermCacheKey(String tenantId, String systemId, String path, String username, Permission perm) {
            this.systemId = systemId;
            this.tenantId = tenantId;
            this.path = path;
            this.username = username;
            this.perm = perm;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getSystemId() {
            return systemId;
        }

        public String getPath() {
            return path;
        }

        public String getUsername() {
            return username;
        }

        public Permission getPerm() { return perm; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FilePermCacheKey that = (FilePermCacheKey) o;

            if (!tenantId.equals(that.tenantId)) return false;
            if (!username.equals(that.username)) return false;
            if (!systemId.equals(that.systemId)) return false;
            if (!path.equals(that.path)) return false;
            return perm == that.perm;
        }

        @Override
        public int hashCode() {
            int result = tenantId.hashCode();
            result = 31 * result + username.hashCode();
            result = 31 * result + systemId.hashCode();
            result = 31 * result + path.hashCode();
            result = 31 * result + perm.hashCode();
            return result;
        }
    }


}
