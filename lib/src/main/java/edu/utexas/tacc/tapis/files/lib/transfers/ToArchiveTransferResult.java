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
    public boolean isComplete() throws ExecutionException, InterruptedException {
        return sourceCommandResult.isDone();

    }

    @Override
    public void waitForCompletion() throws ExecutionException, InterruptedException {
        sourceCommandResult.get();
    }

    @Override
    public String getMessages() throws ExecutionException, InterruptedException {
        SSHCommandResult srcCommandResult = getSourceCommandResult().get();
        byte[] srcCommandOutput = srcCommandResult.getCommandOutput();
        String sourceOutputMessage = (srcCommandOutput == null) ? "" : new String(srcCommandOutput);
        byte[] srcCommandError = srcCommandResult.getCommandError();
        String sourceErrorMessage = (srcCommandError == null) ? "" : new String(srcCommandError);
//        SSHCommandResult dstCommandResult = getDestinationCommandResult().get();
//        byte[] dstCommandOutput = dstCommandResult.getCommandOutput();
//        String destinationOutputMessage = (dstCommandOutput == null) ? "" : new String(dstCommandOutput);
//        byte[] dstCommandError = dstCommandResult.getCommandError();
//        String destinationErrorMessage = (dstCommandError == null) ? "" : new String(dstCommandError);

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
//        if(dstCommandResult.getCommandResult() != 0) {
//            builder.append("DstResult: ");
//            builder.append(dstCommandResult.getCommandResult());
//            builder.append(System.lineSeparator());
//        }
//        if(!StringUtils.isBlank(destinationOutputMessage)) {
//            builder.append("DstOutput: ");
//            builder.append(destinationOutputMessage);
//            builder.append(System.lineSeparator());
//        }
//        if(!StringUtils.isBlank(destinationErrorMessage)) {
//            builder.append("DstError: ");
//            builder.append(destinationErrorMessage);
//            builder.append(System.lineSeparator());
//        }
        ArchiveTransferLog archiveTransferLog = getArchiveTransferLog();

        if(archiveTransferLog != null) {
            builder.append("Files Read:");
            builder.append(System.lineSeparator());
            for (String info : archiveTransferLog.getTransferInfo()) {
                builder.append(info);
            }
        }
        return builder.toString();
    }

    @Override
    public boolean isSuccess() {
        return getCommandResultFromFuture(sourceCommandResult).isSuccess();
    }

    public Future<SSHCommandResult> getSourceCommandResult() {
        return sourceCommandResult;
    }
}
