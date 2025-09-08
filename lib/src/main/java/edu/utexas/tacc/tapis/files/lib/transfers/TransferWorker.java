package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferWorkerConfig;

import java.time.Instant;
import java.util.UUID;

public class TransferWorker {
    private UUID uuid;
    private Instant lastUpdated;

    private TransferWorkerConfig transferWorkerConfig;

    public TransferWorker() {
        this.uuid = uuid;
    }

    public TransferWorker(UUID uuid, Instant lastUpdated, TransferWorkerConfig transferWorkerConfig) {
        this.uuid = uuid;
        this.lastUpdated = lastUpdated;
        this.transferWorkerConfig = transferWorkerConfig;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    public TransferWorkerConfig getTransferWorkerConfig() {
        return transferWorkerConfig;
    }
}
