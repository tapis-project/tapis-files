package edu.utexas.tacc.tapis.files.lib.clients;

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

    public boolean isSuccess() throws ExecutionException, InterruptedException {
        return (sourceCommandResult.get().getCommandResult() == 0) && (destinationCommandResult.get().getCommandResult() == 0);
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
