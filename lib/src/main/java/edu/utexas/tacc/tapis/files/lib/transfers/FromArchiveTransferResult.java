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
/*
    @Override
    public String getMessages() throws ExecutionException, InterruptedException {
        SSHCommandResult dstCommandResult = getDestinationCommandResult().get();
        byte[] dstCommandOutput = dstCommandResult.getCommandOutput();
        String destinationOutputMessage = (dstCommandOutput == null) ? "" : new String(dstCommandOutput);
        byte[] dstCommandError = dstCommandResult.getCommandError();
        String destinationErrorMessage = (dstCommandError == null) ? "" : new String(dstCommandError);

        StringBuilder builder = new StringBuilder();
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

//        if(archiveTransferLog != null) {
//            builder.append("Files Read:");
//            builder.append(System.lineSeparator());
//            for (String info : archiveTransferLog.getTransferInfo()) {
//                builder.append(info);
//            }
//        }
        return builder.toString();
    }
*/
    @Override
    public boolean isSuccess() {
        return getCommandResultFromFuture(destinationCommandResult).isSuccess();
    }

    public Future<SSHCommandResult> getDestinationCommandResult() {
        return destinationCommandResult;
    }
}
