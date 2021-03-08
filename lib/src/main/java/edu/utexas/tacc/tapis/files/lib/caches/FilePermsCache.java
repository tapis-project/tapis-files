package edu.utexas.tacc.tapis.files.lib.caches;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Service
public class FilePermsCache {


    private final SKClient skClient;
    private final ServiceJWT serviceJWT;
    private final TenantManager tenantCache;
    private final IRuntimeConfig config;
    private final LoadingCache<FilePermsCacheKey, Boolean> cache;
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

    public boolean checkPerms(String tenantId, String username, String systemId, String path, FilePermissionsEnum perms) throws ServiceException {
        try {
            FilePermsCacheKey key = new FilePermsCacheKey(tenantId, systemId, path, username, perms);
            return cache.get(key);
        } catch (ExecutionException ex) {
            throw new ServiceException("Could not retrieve system", ex);
        }
    }

    private class PermsLoader extends CacheLoader<FilePermsCacheKey, Boolean> {

        @Override
        public Boolean load(FilePermsCacheKey key) throws Exception {
            Tenant tenant = tenantCache.getTenant(key.getTenantId());
            skClient.setUserAgent("filesServiceClient");
            skClient.setBasePath(tenant.getBaseUrl() + "/v3");
            skClient.addDefaultHeader("x-tapis-token", serviceJWT.getAccessJWT(config.getSiteId()));
            skClient.addDefaultHeader("x-tapis-user", key.getUsername());
            skClient.addDefaultHeader("x-tapis-tenant", key.getTenantId());
            String permSpec = String.format(PERMSPEC, key.getTenantId(), key.getPerms().getLabel(), key.getSystemId(), key.getPath());
            return skClient.isPermitted(key.getTenantId(), key.getUsername(), permSpec);
        }
    }

    private static class FilePermsCacheKey  {
        private final String tenantId;
        private final String username;
        private final String systemId;
        private final String path;
        private final FilePermissionsEnum perms;

        public FilePermsCacheKey(String tenantId, String systemId, String path, String username, FilePermissionsEnum perms) {
            this.systemId = systemId;
            this.tenantId = tenantId;
            this.path = path;
            this.username = username;
            this.perms = perms;
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

        public FilePermissionsEnum getPerms() {
            return perms;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FilePermsCacheKey that = (FilePermsCacheKey) o;

            if (!tenantId.equals(that.tenantId)) return false;
            if (!username.equals(that.username)) return false;
            if (!systemId.equals(that.systemId)) return false;
            if (!path.equals(that.path)) return false;
            return perms == that.perms;
        }

        @Override
        public int hashCode() {
            int result = tenantId.hashCode();
            result = 31 * result + username.hashCode();
            result = 31 * result + systemId.hashCode();
            result = 31 * result + path.hashCode();
            result = 31 * result + perms.hashCode();
            return result;
        }
    }


}
