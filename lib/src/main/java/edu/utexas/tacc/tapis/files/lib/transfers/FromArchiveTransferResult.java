package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.SSHCommandResult;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FromArchiveTransferResult extends ArchiveTransferResult {
    private final Future<SSHCommandResult> destinationCommandResult;

    public FromArchiveTransferResult(Future<SSHCommandResult> destinationCommandResult) {
        this.destinationCommandResult = destinationCommandResult;
    }

    @Override
    public boolean isComplete() {
        return destinationCommandResult.isDone();
    }

    @Override
    public void waitForCompletion() throws Exception {
        destinationCommandResult.get();
    }

    @Override
    public String getMessages() throws ExecutionException, InterruptedException {
        return getMessages(destinationCommandResult, "Destination");
    }

    @Override
    public boolean isSuccess() {
        return getCommandResultFromFuture(destinationCommandResult).isSuccess();
    }

    public Future<SSHCommandResult> getDestinationCommandResult() {
        return destinationCommandResult;
    }
}
