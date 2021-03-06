package edu.utexas.tacc.tapis.files.lib.caches;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Service
public class SystemsCache {

    private static final Logger log = LoggerFactory.getLogger(SystemsCache.class);
    private final IRuntimeConfig config;
    private final LoadingCache<SystemCacheKey, TapisSystem> cache;
    private final ServiceClients serviceClients;

    @Inject
    public SystemsCache(ServiceClients serviceClients) {
        this.serviceClients = serviceClients;
        this.config = RuntimeSettings.get();
        cache = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(5))
            .build(new SystemLoader());
    }


    public TapisSystem getSystem(String tenantId, String systemId, String username) throws ServiceException {
        try {
            SystemCacheKey key = new SystemCacheKey(tenantId, systemId, username);
            TapisSystem system = cache.get(key);
            return system;
        } catch (ExecutionException ex) {
            String msg = Utils.getMsg("FILES_CACHE_ERR", "Systems", tenantId, systemId, username, ex.getMessage());
            throw new ServiceException(msg, ex);
        }
    }

    private class SystemLoader extends CacheLoader<SystemCacheKey, TapisSystem> {

        @Override
        public TapisSystem load(SystemCacheKey key) throws Exception {
            SystemsClient client = serviceClients.getClient(key.getUsername(), key.getTenantId(), SystemsClient.class);
            TapisSystem system = client.getSystemWithCredentials(key.getSystemId(), null);
            return system;
        }
    }

    private static class SystemCacheKey {
        private final String tenantId;
        private final String systemId;
        private final String username;

        public SystemCacheKey(String tenantId, String systemId, String username) {
            this.systemId = systemId;
            this.tenantId = tenantId;
            this.username = username;
        }

        public String getTenantId() {
            return tenantId;
        }

        public String getSystemId() {
            return systemId;
        }

        public String getUsername() {
            return username;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SystemCacheKey that = (SystemCacheKey) o;

            if (!tenantId.equals(that.tenantId)) return false;
            if (!systemId.equals(that.systemId)) return false;
            return username.equals(that.username);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tenantId, systemId, username);

        }
    }

}
