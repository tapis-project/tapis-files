package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.util.List;

public class TestFileTransfersConfig extends BaseTransfersIntegrationConfig {

    int maxThreads;
    private long timeout = 10000;
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
}
