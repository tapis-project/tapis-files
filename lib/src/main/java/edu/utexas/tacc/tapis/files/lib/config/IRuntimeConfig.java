package edu.utexas.tacc.tapis.files.lib.config;

import edu.utexas.tacc.tapis.files.lib.models.TransferWorkerConfig;

import java.util.Set;

public interface IRuntimeConfig {
    long getRequiredPostgresVersion();
    String getHostName();
    String getDbUsername();
    String getDbUrl();
    String getDbPassword();
    String getServicePassword();
    String getTokensServiceURL();
    String getTenantsServiceURL();
    String getSiteId();
    String getGlobusClientId();
    int getChildThreadPoolSize();
    int getParentThreadPoolSize();
    int getArchiveTransferThreadPoolSize();
    int getAsyncTransferPollSeconds();
    int getPostItsReaperIntervalMinutes();
    int getDbConnectionPoolCoreSize();
    int getDbConnectionPoolSize();
    int getSshPoolTraceOnCleanupInterval();
    int getSshPoolApiMaxConnectionsPerKey();
    int getSshPoolApiMaxSessionsPerConnection();
    int getSshPoolApiMaxSessionLifetimeMillis();
    int getSshPoolWorkerMaxConnectionsPerKey();
    int getSshPoolWorkerMaxSessionsPerConnection();
    int getSshPoolWorkerMaxSessionLifetimeMillis();
    int getMaxTransferCount();
    int getMaxAssignmentWaitMultiplier();
    boolean isAuditingEnabled();
    public Set<TransferWorkerConfig.TransferType> getWorkerAcceptedTransferTypes();
    public int getGrizzlyPoolCoreSize();
    public int getGrizzlyPoolMaxSize();
    public String getTapisDebugSystemServicePath();
}
