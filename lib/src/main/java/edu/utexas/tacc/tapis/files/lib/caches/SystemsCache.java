package edu.utexas.tacc.tapis.files.lib.caches;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Service
public class SystemsCache {


    private final SystemsClient systemsClient;
    private final ServiceJWT serviceJWT;
    private final IRuntimeConfig config;
    private final LoadingCache<SystemCacheKey, TSystem> cache;

    @Inject
    public SystemsCache(SystemsClient systemsClient, ServiceJWT serviceJWT) {
        this.systemsClient = systemsClient;
        this.serviceJWT = serviceJWT;
        this.config = RuntimeSettings.get();
        cache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .build(new SystemLoader());
    }


    public TSystem getSystem(String tenantId, String systemId) throws ServiceException {
        try {
            SystemCacheKey key = new SystemCacheKey(tenantId, systemId);
            return cache.get(key);
        } catch (ExecutionException ex) {
            throw new ServiceException("Could not retrieve system", ex);
        }
    }

    private class SystemLoader extends CacheLoader<SystemCacheKey, TSystem> {

        @Override
        public TSystem load(SystemCacheKey key) throws Exception {
            systemsClient.addDefaultHeader("x-tapis-token", serviceJWT.getAccessJWT(config.getSiteId()));
            systemsClient.addDefaultHeader("x-tapis-tenant", key.getTenantId());
            return systemsClient.getSystemWithCredentials(key.getSystemId(), null);
        }
    }

    private static class SystemCacheKey {
        private final String tenantId;
        private final String systemId;

        public SystemCacheKey(String tenantId, String systemId) {
            this.systemId = systemId;
            this.tenantId = tenantId;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getSystemId() {
            return systemId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SystemCacheKey that = (SystemCacheKey) o;
            if (!Objects.equals(tenantId, that.tenantId)) return false;
            return Objects.equals(systemId, that.systemId);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (systemId != null ? systemId.hashCode() : 0);
            return result;
        }
    }


}
