package edu.utexas.tacc.tapis.files.lib.models;

public enum ArchiveTransferStatus {
    ACCEPTED(false),
    AWAITING_RETRY(false),
    IN_PROGRESS(false),
    COMPLETED(true),
    FAILED(true);

    boolean finalState = false;

    ArchiveTransferStatus(boolean finalState) {
        this.finalState = finalState;
    }
    public boolean isFinalState() {
        return this.finalState;
    }
}
