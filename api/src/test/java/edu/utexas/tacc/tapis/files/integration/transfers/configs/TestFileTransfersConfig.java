package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.util.List;

public class TestFileTransfersConfig extends BaseTransfersIntegrationConfig {

    int maxThreads;

    // default to waiting 10 seconds
    private long timeout = 10000;

    // default to waiting 1 second
    private int pollingIntervalMillis = 1000;

    List<TransfersConfig> transfers;

    public List<TransfersConfig> getTransfers() {
        return transfers;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public long getTimeout() {
        return timeout;
    }

    public int getPollingIntervalMillis() {
        return pollingIntervalMillis;
    }

}
