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

    /*
    public String getMessages() throws ExecutionException, InterruptedException {
        SSHCommandResult srcCommandResult = getSourceCommandResult().get();
        byte[] srcCommandOutput = srcCommandResult.getCommandOutput();
        String sourceOutputMessage = (srcCommandOutput == null) ? "" : new String(srcCommandOutput);
        byte[] srcCommandError = srcCommandResult.getCommandError();
        String sourceErrorMessage = (srcCommandError == null) ? "" : new String(srcCommandError);
        SSHCommandResult dstCommandResult = getDestinationCommandResult().get();
        byte[] dstCommandOutput = dstCommandResult.getCommandOutput();
        String destinationOutputMessage = (dstCommandOutput == null) ? "" : new String(dstCommandOutput);
        byte[] dstCommandError = dstCommandResult.getCommandError();
        String destinationErrorMessage = (dstCommandError == null) ? "" : new String(dstCommandError);

        StringBuilder builder = new StringBuilder();
        if(srcCommandResult.getCommandResult() != 0) {
            builder.append("SrcResult: ");
            builder.append(srcCommandResult.getCommandResult());
            builder.append(System.lineSeparator());
        }
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
        if(dstCommandResult.getCommandResult() != 0) {
            builder.append("DstResult: ");
            builder.append(dstCommandResult.getCommandResult());
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
        ArchiveTransferLog archiveTransferLog = getArchiveTransferLog();
        return builder.toString();
    }
     */

    public boolean isSuccess() {
        return(getCommandResultFromFuture(sourceCommandResult).isSuccess() &&
                getCommandResultFromFuture(destinationCommandResult).isSuccess());
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
