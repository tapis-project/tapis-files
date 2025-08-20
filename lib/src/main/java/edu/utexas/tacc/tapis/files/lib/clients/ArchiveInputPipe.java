package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Future;

public class ArchiveInputPipe extends PipedInputStream {
    private Future<SSHCommandResult> sourceResultFuture;
    ArchiveInputPipe(PipedOutputStream src) throws IOException {
        super(src);
    }
    ArchiveInputPipe(PipedOutputStream src, int pipeSize) throws IOException {
        super(src, pipeSize);
    }

    public void setSourceResult(Future<SSHCommandResult> sourceResultFuture) {
        this.sourceResultFuture = sourceResultFuture;
    }

    public Future<SSHCommandResult> getSourceResultFuture() {
        return sourceResultFuture;
    }


}
