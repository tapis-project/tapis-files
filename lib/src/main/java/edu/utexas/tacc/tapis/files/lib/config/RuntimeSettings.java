package edu.utexas.tacc.tapis.files.lib.config;

import org.apache.commons.lang3.StringUtils;

public class RuntimeSettings {

    private static final Settings settings = new Settings();

    static class BaseConfig implements IRuntimeConfig{

        protected String hostName = settings.get("TAPIS_LOCAL_NODE_NAME", "devHost");
        protected String siteId = settings.get("TAPIS_SITE_ID", ""); // Site ID must be set in environment
        protected String dbHost = settings.get("DB_HOST", "localhost");
        protected String dbName = settings.get("DB_NAME", "dev");
        protected String dbUsername = settings.get("DB_USERNAME", "dev");
        protected String dbPassword = settings.get("DB_PASSWORD", "dev");
        protected String dbPort = settings.get("DB_PORT", "5432");
        protected String rabbitMQHost = settings.get("RABBITMQ_HOSTNAME", "localhost");
        protected String rabbitMQUsername = settings.get("RABBITMQ_USERNAME", "dev");
        protected String rabbitMQVHost = settings.get("RABBITMQ_VHOST", "dev");
        protected String rabbitmqPassword = settings.get("RABBITMQ_PASSWORD", "dev");
        protected String servicePassword = settings.get("SERVICE_PASSWORD", "dev");
        protected String tokensServiceURL = settings.get("TOKENS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String tenantsServiceURL = settings.get("TENANTS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String globusClientId = settings.get("TAPIS_GLOBUS_CLIENT_ID", "");
        // How often to poll when monitoring an asynchronous transfer. Default is 120 seconds.
        protected final int asyncTransferPollSeconds = getIntSetting("ASYNC_TRANSFER_POLL_SECONDS", 120);
        protected final int postItsReaperIntervalMinutes = getIntSetting("POSTITS_REAPER_INTERVAL_MINUTES", 1440);
        protected final int dbConnectionPoolCoreSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_CORE_SIZE", 15);
        protected final int dbConnectionPoolSize = getIntSetting("TAPIS_DB_CONNECTION_POOL_SIZE", 20);
        protected final int sshPoolTraceOnCleanupInterval = getIntSetting("TAPIS_SSH_POOL_TRACE_ON_CLEANUP_INTERVAL", 4);
        protected final int rereadLogConfigIntevalSeconds = getIntSetting("TAPIS_REREAD_LOG_CONFIG_INTERVAL_SECONDS", 300);

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

        public int getSshPoolTraceOnCleanupInterval() {
            return sshPoolTraceOnCleanupInterval;
        }

        public int getRereadLogConfigIntevalSeconds() {
            return rereadLogConfigIntevalSeconds;
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
