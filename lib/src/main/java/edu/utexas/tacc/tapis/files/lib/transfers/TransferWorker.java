package edu.utexas.tacc.tapis.files.lib.transfers;

import java.time.Instant;
import java.util.UUID;

public class TransferWorker {
    private UUID uuid;
    private Instant lastUpdated;

    public TransferWorker() {
        this.uuid = uuid;
    }

    public TransferWorker(UUID uuid, Instant lastUpdated) {
        this.uuid = uuid;
        this.lastUpdated = lastUpdated;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }
}
