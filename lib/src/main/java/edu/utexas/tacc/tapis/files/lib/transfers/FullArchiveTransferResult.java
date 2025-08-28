package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.SSHCommandResult;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FullArchiveTransferResult extends ArchiveTransferResult {
    private final Future<SSHCommandResult> sourceCommandResult;
    private final Future<SSHCommandResult> destinationCommandResult;
    public FullArchiveTransferResult(Future<SSHCommandResult> sourceCommandResult, Future<SSHCommandResult> destinationCommandResult) {
        this.sourceCommandResult = sourceCommandResult;
        this.destinationCommandResult = destinationCommandResult;
    }

    public Future<SSHCommandResult> getSourceCommandResult() {
        return sourceCommandResult;
    }

    public Future<SSHCommandResult> getDestinationCommandResult() {
        return destinationCommandResult;
    }

    @Override
    public boolean isComplete() {
        return(sourceCommandResult.isDone() && destinationCommandResult.isDone());
    }

    public void waitForCompletion() throws ExecutionException, InterruptedException {
        sourceCommandResult.get();
        destinationCommandResult.get();
    }

    @Override
    public String getMessages() throws ExecutionException, InterruptedException {
        return getMessages(sourceCommandResult, "Source") + System.lineSeparator() +
                getMessages(destinationCommandResult, "Destination");
    }

    public boolean isSuccess() {
        return(getCommandResultFromFuture(sourceCommandResult).isSuccess() &&
                getCommandResultFromFuture(destinationCommandResult).isSuccess());
    }
}
