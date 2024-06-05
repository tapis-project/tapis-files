package edu.utexas.tacc.tapis.files.lib.config;

import org.apache.commons.lang3.StringUtils;

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
        protected String rabbitMQHost = settings.get("RABBITMQ_HOSTNAME");
        protected String rabbitMQUsername = settings.get("RABBITMQ_USERNAME");
        protected String rabbitMQVHost = settings.get("RABBITMQ_VHOST");
        protected String rabbitmqPassword = settings.get("RABBITMQ_PASSWORD");
        protected String servicePassword = settings.get("SERVICE_PASSWORD");
        protected String tokensServiceURL = settings.get("TOKENS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String tenantsServiceURL = settings.get("TENANTS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String globusClientId = settings.get("TAPIS_GLOBUS_CLIENT_ID", "");
        protected final int parentThreadPoolSize = getIntSetting("PARENT_THREAD_POOL_SIZE", 8);
        protected final int childThreadPoolSize = getIntSetting("CHILD_THREAD_POOL_SIZE", 20);
        // How often to poll when monitoring an asynchronous transfer. Default is 120 seconds.
        protected final int asyncTransferPollSeconds = getIntSetting("ASYNC_TRANSFER_POLL_SECONDS", 120);
        protected final int postItsReaperIntervalMinutes = getIntSetting("POSTITS_REAPER_INTERVAL_MINUTES", 1440);
        protected final int dbConnectionPoolCoreSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_CORE_SIZE", 15);
        protected final int dbConnectionPoolSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_SIZE", 20);
        protected final int sshPoolTraceOnCleanupInterval = getIntSetting("TAPIS_SSH_POOL_TRACE_ON_CLEANUP_INTERVAL", 4);
        protected final int sshPoolApiMaxConnectionsPerKey = getIntSetting("TAPIS_SSH_POOL_API_MAX_CONNECTIONS_PER_KEY", 8);
        protected final int sshPoolApiMaxSessionsPerConnection = getIntSetting("TAPIS_SSH_POOL_API_MAX_SESSIONS_PER_CONNECTION", 10);
        protected final int sshPoolApiMaxSessionLifetimeMillis = getIntSetting("TAPIS_SSH_POOL_API_MAX_SESSION_LIFETIME_MILLIS", 300000);
        protected final int sshPoolWorkerMaxConnectionsPerKey = getIntSetting("TAPIS_SSH_POOL_WORKER_MAX_CONNECTIONS_PER_KEY", 8);
        protected final int sshPoolWorkerMaxSessionsPerConnection = getIntSetting("TAPIS_SSH_POOL_WORKER_MAX_SESSIONS_PER_CONNECTION", 10);
        protected final int sshPoolWorkerMaxSessionLifetimeMillis = getIntSetting("TAPIS_SSH_POOL_API_MAX_SESSION_LIFETIME_MILLIS", 300000);
        protected final int grizzlyPoolCoreSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_CORE_SIZE", 40);
        protected final int grizzlyPoolMaxSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_SIZE", 50);
        protected final String tapisDebugSystemServicePath = settings.get("TAPIS_DEBUG_SYSTEM_SERVICE_PATH", null);
        protected final int maxTransferCount = getIntSetting("MAX_TRANSFER_COUNT", 10000);

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

        public String getRabbitMQHost() {
            return rabbitMQHost;
        }

        public String getRabbitMQUsername() {
            return rabbitMQUsername;
        }

        public String getRabbitMQVHost() {
            return rabbitMQVHost;
        }

        public String getRabbitmqPassword() {
            return rabbitmqPassword;
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

        public static int getIntSetting(String settingName, int defaultValue) {
            String settingValue = settings.get(settingName);
            if(StringUtils.isBlank(settingValue)) {
                return defaultValue;
            }
            return Integer.parseInt(settingValue);
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
