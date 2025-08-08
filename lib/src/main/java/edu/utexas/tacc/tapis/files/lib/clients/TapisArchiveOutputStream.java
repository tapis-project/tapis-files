package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.PipedOutputStream;
import java.util.concurrent.Future;

public class TapisArchiveOutputStream extends PipedOutputStream {
    private Future<SSHCommandResult> sourceResultFuture;

    public void setSourceResult(Future<SSHCommandResult> sourceResultFuture) {
        this.sourceResultFuture = sourceResultFuture;
    }

    public Future<SSHCommandResult> getSourceResultFuture() {
        return sourceResultFuture;
    }
}
