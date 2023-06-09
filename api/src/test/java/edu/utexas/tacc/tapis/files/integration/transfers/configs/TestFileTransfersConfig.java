package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.util.List;

public class TestFileTransfersConfig extends BaseTransfersIntegrationConfig {

    List<TransfersConfig> transfersConfig;

    public List<TransfersConfig> getTransfersConfig() {
        return transfersConfig;
    }

    public void setTransfersConfig(List<TransfersConfig> transfersConfig) {
        this.transfersConfig = transfersConfig;
    }
}
