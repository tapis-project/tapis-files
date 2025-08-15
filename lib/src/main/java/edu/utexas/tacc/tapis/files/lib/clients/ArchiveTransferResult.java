package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ArchiveTransferResult {
    private final Future<SSHCommandResult> sourceCommandResult;
    private final Future<SSHCommandResult> destinationCommandResult;

    ArchiveTransferResult(Future<SSHCommandResult> sourceCommandResult, Future<SSHCommandResult> destinationCommandResult) {
        this.sourceCommandResult = sourceCommandResult;
        this.destinationCommandResult = destinationCommandResult;
    }

    public Future<SSHCommandResult> getSourceCommandResult() {
        return sourceCommandResult;
    }

    public Future<SSHCommandResult> getDestinationCommandResult() {
        return destinationCommandResult;
    }

    public void waitForCompletion() throws ExecutionException, InterruptedException {
        sourceCommandResult.get();
        destinationCommandResult.get();
    }
    public String getMessages() throws ExecutionException, InterruptedException {
        byte[] srcCommandOutput = getSourceCommandResult().get().getCommandOutput();
        String sourceOutputMessage = (srcCommandOutput == null) ? "" : new String(srcCommandOutput);
        byte[] srcCommandError = getSourceCommandResult().get().getCommandError();
        String sourceErrorMessage = (srcCommandError == null) ? "" : new String(srcCommandError);
        byte[] dstCommandOutput = getSourceCommandResult().get().getCommandOutput();
        String destinationOutputMessage = (dstCommandOutput == null) ? "" : new String(dstCommandOutput);
        byte[] dstCommandError = getDestinationCommandResult().get().getCommandError();
        String destinationErrorMessage = (dstCommandError == null) ? "" : new String(dstCommandError);

        StringBuilder builder = new StringBuilder();
        if(!StringUtils.isBlank(sourceOutputMessage)) {
            builder.append("SrcOutput: ");
            builder.append(sourceOutputMessage);
            builder.append(System.lineSeparator());
        }
        if(!StringUtils.isBlank(sourceErrorMessage)) {
            builder.append("SrcError: ");
            builder.append(sourceErrorMessage);
            builder.append(System.lineSeparator());
        }
        if(!StringUtils.isBlank(destinationOutputMessage)) {
            builder.append("DstOutput: ");
            builder.append(destinationOutputMessage);
            builder.append(System.lineSeparator());
        }
        if(!StringUtils.isBlank(destinationErrorMessage)) {
            builder.append("DstError: ");
            builder.append(destinationErrorMessage);
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }

    public boolean isSuccess() {
        return(getCommandResultFromFuture(sourceCommandResult).isSuccess() &&
                getCommandResultFromFuture(destinationCommandResult).isSuccess());
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

    @Override
    public String toString() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Src: ");
            sb.append(sourceCommandResult.get());
            sb.append(System.lineSeparator());
            sb.append("Dst: ");
            sb.append(destinationCommandResult.get());
            sb.append(System.lineSeparator());
            return sb.toString();
        } catch (Exception ex) {
            return ex.getStackTrace().toString();
        }
    }
}
