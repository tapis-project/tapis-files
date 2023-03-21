package edu.utexas.tacc.tapis.files.lib.config;

public interface IRuntimeConfig {
    String getHostName();
    String getDbUsername();
    String getDbHost();
    String getDbName();
    String getDbPassword();
    String getDbPort();
    String getRabbitMQUsername();
    String getRabbitMQVHost();
    String getRabbitmqPassword();
    String getRabbitMQHost();
    String getServicePassword();
    String getTokensServiceURL();
    String getTenantsServiceURL();
    String getSiteId();
    int getPostItsReaperIntervalMinutes();
    int getDbConnectionPoolCoreSize();
    int getDbConnectionPoolSize();
}
