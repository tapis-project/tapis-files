package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.SSHCommandResult;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ToArchiveTransferResult extends ArchiveTransferResult {
    private final Future<SSHCommandResult> sourceCommandResult;

    public ToArchiveTransferResult(Future<SSHCommandResult> sourceCommandResult) {
        this.sourceCommandResult = sourceCommandResult;
    }

    @Override
    public boolean isComplete() {
        return sourceCommandResult.isDone();

    }

    @Override
    public void waitForCompletion() throws ExecutionException, InterruptedException {
        sourceCommandResult.get();
    }

    @Override
    public String getMessages() throws ExecutionException, InterruptedException {
        return getMessages(sourceCommandResult, "Source");
    }

    @Override
    public boolean isSuccess() {
        return getCommandResultFromFuture(sourceCommandResult).isSuccess();
    }

    public Future<SSHCommandResult> getSourceCommandResult() {
        return sourceCommandResult;
    }
}
