package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.SSHCommandResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class ArchiveTransferResult {
    private ArchiveTransferLog archiveTransferLog;
    public abstract boolean isComplete();
    public abstract void waitForCompletion() throws Exception;
    public abstract String getMessages() throws ExecutionException, InterruptedException;
    public abstract boolean isSuccess();

    public ArchiveTransferLog getArchiveTransferLog() {
        return archiveTransferLog;
    }

    public void setArchiveTransferLog(ArchiveTransferLog archiveTransferLog) {
        this.archiveTransferLog = archiveTransferLog;
    }

    SSHCommandResult getCommandResultFromFuture(Future<SSHCommandResult> future) {
        if(future.isDone()) {
            try {
                return future.get();
            } catch (Exception ex) {
                // I don't think this can happen since we know they are complete, but handle it anyway.
                throw new RuntimeException("Exception getting command result", ex);
            }
        } else {
            throw new RuntimeException("Command result is not complete.");
        }
    }
}
