package edu.utexas.tacc.tapis.files.lib.models;

public enum ArchiveTransferStatus {
    ACCEPTED(false),
//    STAGING,
//    STAGED,
    AWAITING_RETRY(false),
    IN_PROGRESS(false),
    COMPLETED(true),
//    CANCELLED,
    FAILED(true);
//    FAILED_OPT,
//    PAUSED,
//    UNKNOWN

    boolean finalState = false;

    ArchiveTransferStatus(boolean finalState) {
        this.finalState = finalState;
    }
    public boolean isFinalState() {
        return this.finalState;
    }
}
