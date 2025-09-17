package edu.utexas.tacc.tapis.files.lib.models;

import java.util.HashSet;
import java.util.Set;

public enum ArchiveTransferStatus {
    ACCEPTED(false),
    AWAITING_RETRY(false),
    IN_PROGRESS(false),
    COMPLETED(true),
    CANCELLED(true),
    FAILED(true);

    boolean finalState = false;
    ArchiveTransferStatus(boolean finalState) {
        this.finalState = finalState;
    }
    public boolean isFinalState() {
        return this.finalState;
    }

    public static Set<ArchiveTransferStatus> getFinalStates() {
        Set<ArchiveTransferStatus> finalStates = new HashSet<>();

        for(ArchiveTransferStatus status : ArchiveTransferStatus.values()) {
            if(status.isFinalState()) {
                finalStates.add(status);
            }
        }

        return finalStates;
    }
}
