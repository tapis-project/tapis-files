package edu.utexas.tacc.tapis.files.lib.config;

public class RuntimeSettings {

    private static Settings settings = new Settings();

    static class BaseConfig implements IRuntimeConfig{

        protected String siteId = settings.get("TAPIS_SITE_ID");
        protected String dbHost = settings.get("DB_HOST", "localhost");
        protected String dbName = settings.get("DB_NAME", "dev");
        protected String dbUsername = settings.get("DB_USERNAME", "dev");
        protected String dbPassword = settings.get("DB_PASSWORD", "dev");
        protected String dbPort = settings.get("DB_PORT", "5432");
        protected String rabbitMQUsername = settings.get("RABBITMQ_USERNAME", "dev");
        protected String rabbitMQVHost = settings.get("RABBITMQ_VHOST", "dev");
        protected String rabbitmqPassword = settings.get("RABBITMQ_PASSWORD", "dev");
        protected String servicePassword = settings.get("SERVICE_PASSWORD", "dev");
        protected String tokensServiceURL = settings.get("TOKENS_SERVICE_URL", "https://dev.develop.tapis.io");
        protected String tenantsServiceURL = settings.get("TENANTS_SERVICE_URL", "https://dev.develop.tapis.io");

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
