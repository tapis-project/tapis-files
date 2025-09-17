package edu.utexas.tacc.tapis.files.lib.config;

import edu.utexas.tacc.tapis.files.lib.models.TransferWorkerConfig;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class RuntimeSettings {

    private static final Settings settings = new Settings();

    static class BaseConfig implements IRuntimeConfig{

        protected String hostName = settings.get("TAPIS_LOCAL_NODE_NAME", "devHost");
        protected String siteId = settings.get("TAPIS_SITE_ID"); // Site ID must be set in environment
        protected String dbHost = settings.get("DB_HOST");
        protected String dbName = settings.get("DB_NAME");
        protected String dbUsername = settings.get("DB_USERNAME");
        protected String dbPassword = settings.get("DB_PASSWORD");
        protected String dbPort = settings.get("DB_PORT", "5432");
        protected String servicePassword = settings.get("SERVICE_PASSWORD");
        protected String tokensServiceURL = settings.get("TOKENS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String tenantsServiceURL = settings.get("TENANTS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String globusClientId = settings.get("TAPIS_GLOBUS_CLIENT_ID", "");
        protected final int parentThreadPoolSize = getIntSetting("PARENT_THREAD_POOL_SIZE", 24);
        protected final int childThreadPoolSize = getIntSetting("CHILD_THREAD_POOL_SIZE", 32);
        protected final int archiveTransferThreadPoolSize = getIntSetting("ARCHIVE_TRANSFER_THREAD_POOL_SIZE", 15);
        // How often to poll when monitoring an asynchronous transfer. Default is 120 seconds.
        protected final int asyncTransferPollSeconds = getIntSetting("ASYNC_TRANSFER_POLL_SECONDS", 120);
        protected final int postItsReaperIntervalMinutes = getIntSetting("POSTITS_REAPER_INTERVAL_MINUTES", 1440);
        protected final int dbConnectionPoolCoreSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_CORE_SIZE", 15);
        protected final int dbConnectionPoolSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_SIZE", 20);
        protected final int sshPoolTraceOnCleanupInterval = getIntSetting("TAPIS_SSH_POOL_TRACE_ON_CLEANUP_INTERVAL", 4);
        protected final int sshPoolApiMaxConnectionsPerKey = getIntSetting("TAPIS_SSH_POOL_API_MAX_CONNECTIONS_PER_KEY", 8);
        protected final int sshPoolApiMaxSessionsPerConnection = getIntSetting("TAPIS_SSH_POOL_API_MAX_SESSIONS_PER_CONNECTION", 10);
        protected final int sshPoolApiMaxSessionLifetimeMillis = getIntSetting("TAPIS_SSH_POOL_API_MAX_SESSION_LIFETIME_MILLIS", 300000);
        protected final int sshPoolWorkerMaxConnectionsPerKey = getIntSetting("TAPIS_SSH_POOL_WORKER_MAX_CONNECTIONS_PER_KEY", 25);
        protected final int sshPoolWorkerMaxSessionsPerConnection = getIntSetting("TAPIS_SSH_POOL_WORKER_MAX_SESSIONS_PER_CONNECTION", 10);
        protected final int sshPoolWorkerMaxSessionLifetimeMillis = getIntSetting("TAPIS_SSH_POOL_API_MAX_SESSION_LIFETIME_MILLIS", 300000);
        protected final int grizzlyPoolCoreSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_CORE_SIZE", 40);
        protected final int grizzlyPoolMaxSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_SIZE", 50);
        protected final String tapisDebugSystemServicePath = settings.get("TAPIS_DEBUG_SYSTEM_SERVICE_PATH", null);
        protected final int maxTransferCount = getIntSetting("MAX_TRANSFER_COUNT", 10000);
        protected final int maxAssignmentWaitMultiplier = getIntSetting("MAX_ASSIGNMENT_WAIT_MULTIPLIER", 5);
        protected final boolean auditingEnabled = getBoolSetting("TAPIS_AUDITING_ENABLED", false);
        protected final Set<TransferWorkerConfig.TransferType> workerAcceptedTransferTypes = getSetSetting("TAPIS_FILES_WORKER_ACCEPTED_TRANSFER_TYPES",
                Collections.emptySet(), TransferWorkerConfig.TransferType.class, value -> TransferWorkerConfig.TransferType.valueOf(value.trim()));
        protected final long requiredPostgresVersion = 160003;

        public long getRequiredPostgresVersion() {
            return requiredPostgresVersion;
        }

        public String getHostName() {
            return hostName;
        }

        public String getDbHost() {
            return dbHost;
        }

        public String getDbName() {
            return dbName;
        }

        public String getDbUsername() {
            return dbUsername;
        }

        public String getDbPassword() {
            return dbPassword;
        }

        public String getDbPort() {
            return dbPort;
        }

        public String getServicePassword() { return servicePassword; }

        public String getTokensServiceURL() { return tokensServiceURL; }

        public String getTenantsServiceURL() { return tenantsServiceURL; }

        public String getSiteId() { return siteId; }

        public String getGlobusClientId() { return globusClientId; }

        public int getChildThreadPoolSize() {
            return childThreadPoolSize;
        }

        public int getParentThreadPoolSize() {
            return parentThreadPoolSize;
        }

        @Override
        public int getArchiveTransferThreadPoolSize() {
            return archiveTransferThreadPoolSize;
        }

        public int getAsyncTransferPollSeconds() { return asyncTransferPollSeconds; }

        public int getPostItsReaperIntervalMinutes() {
            return postItsReaperIntervalMinutes;
        }

        public int getDbConnectionPoolCoreSize() {
            return dbConnectionPoolCoreSize;
        }

        public int getDbConnectionPoolSize() {
            return dbConnectionPoolSize;
        }

        public int getGrizzlyPoolCoreSize() {
            return grizzlyPoolCoreSize;
        }

        public int getGrizzlyPoolMaxSize() {
            return grizzlyPoolMaxSize;
        }

        public String getTapisDebugSystemServicePath() {
            return tapisDebugSystemServicePath;
        }

        public int getSshPoolTraceOnCleanupInterval() {
            return sshPoolTraceOnCleanupInterval;
        }

        public int getSshPoolApiMaxSessionLifetimeMillis() {
            return sshPoolApiMaxSessionLifetimeMillis;
        }
        public int getSshPoolApiMaxConnectionsPerKey() {
            return sshPoolApiMaxConnectionsPerKey;
        }

        public int getSshPoolApiMaxSessionsPerConnection() {
            return sshPoolApiMaxSessionsPerConnection;
        }

        public int getSshPoolWorkerMaxConnectionsPerKey() {
            return sshPoolWorkerMaxConnectionsPerKey;
        }

        public int getSshPoolWorkerMaxSessionsPerConnection() {
            return sshPoolWorkerMaxSessionsPerConnection;
        }

        public int getSshPoolWorkerMaxSessionLifetimeMillis() {
            return sshPoolWorkerMaxSessionLifetimeMillis;
        }

        public int getMaxTransferCount() {
            return maxTransferCount;
        }
        public int getMaxAssignmentWaitMultiplier() {
            return maxAssignmentWaitMultiplier;
        }

        public boolean isAuditingEnabled() { return auditingEnabled; }

        public Set<TransferWorkerConfig.TransferType> getWorkerAcceptedTransferTypes() {
            return workerAcceptedTransferTypes;
        }

        public static int getIntSetting(String settingName, int defaultValue) {
            String settingValue = settings.get(settingName);
            if(StringUtils.isBlank(settingValue)) {
                return defaultValue;
            }
            return Integer.parseInt(settingValue);
        }
        public static boolean getBoolSetting(String settingName, boolean defaultValue) {
            String settingValue = settings.get(settingName);
            if (StringUtils.isBlank(settingValue)) return defaultValue;
            return Boolean.parseBoolean(settingValue);
        }

        public static <T> Set<T> getSetSetting(String settingName, Set<T> defaultValue, Class<T> clazz, Function<String, T> objectFromString) {
            String settingValue = settings.get(settingName);
            if (StringUtils.isBlank(settingValue)) return defaultValue;
            String[] stringArray = Strings.split(settingValue, ',');

            Set<T> newSet = new HashSet<>();
            for(String stringArrayElement : stringArray) {
                newSet.add(objectFromString.apply(stringArrayElement));
            }
            return newSet;
        }
    }

    private static class TestConfig extends BaseConfig {
        protected String dbHost = settings.get("DB_HOST", "localhost");
        protected String dbName = "test";
        protected String dbUsername = "test";
        protected String dbPassword = "test";
        protected String dbPort = "5432";

        @Override
        public String getDbName() { return dbName; }

        @Override
        public String getDbHost() {
            return dbHost;
        }

        @Override
        public String getDbUsername() {
            return dbUsername;
        }

        @Override
        public String getDbPassword() {
            return dbPassword;
        }

        @Override
        public String getDbPort() {
            return dbPort;
        }
    }


    public static IRuntimeConfig get() {
        if (settings.get("APP_ENV", "dev").equalsIgnoreCase("dev")) {
            return new BaseConfig();
        } else if (settings.get("APP_ENV", "dev").equalsIgnoreCase("test")) {
            return new TestConfig();
        } else {
            //TODO:
            return new BaseConfig();
        }
    }
}
