package edu.utexas.tacc.tapis.files.lib.config;

public class RuntimeSettings {

    private static Settings settings = new Settings();

    static class BaseConfig implements IRuntimeConfig{
        private String dbHost = settings.get("DB_HOST", "localhost");
        private String dbName = settings.get("DB_NAME", "dev");
        private String dbUsername = settings.get("DB_USERNAME", "dev");
        private String dbPassword = settings.get("DB_PASSWORD", "dev");
        private String dbPort = settings.get("DB_PORT", "5432");
        private String rabbitMQUsername = settings.get("RABBITMQ_USERNAME", "dev");
        private String rabbitMQVHost = settings.get("RABBITMQ_VHOST", "dev");
        private String rabbitmqPassword = settings.get("RABBITMQ_PASSWORD", "dev");

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
    }


    private static class TestConfig extends BaseConfig {
        private String dbHost = settings.get("DB_HOST", "localhost");
        private String dbUsername = "test";
        private String dbPassword = "test";
        private String dbPort = "5432";
    }

    public static IRuntimeConfig get() {
        if (settings.get("APP_ENV", "dev").equalsIgnoreCase("dev")) {
            return new BaseConfig();
        } else if (settings.get("APP_ENV", "dev").equalsIgnoreCase("test")) {
            return new TestConfig();
        } else {
            return new BaseConfig();
        }
    }

}
