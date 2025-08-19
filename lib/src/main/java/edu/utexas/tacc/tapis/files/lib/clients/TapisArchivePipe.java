package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.channels.Pipe;
import java.util.concurrent.Future;

public class TapisArchivePipe extends PipedInputStream {
    private Future<SSHCommandResult> sourceResultFuture;
    TapisArchivePipe(PipedOutputStream src) throws IOException {
        super(src);
    }
    TapisArchivePipe(PipedOutputStream src, int pipeSize) throws IOException {
        super(src, pipeSize);
    }

    public void setSourceResult(Future<SSHCommandResult> sourceResultFuture) {
        this.sourceResultFuture = sourceResultFuture;
    }

    public Future<SSHCommandResult> getSourceResultFuture() {
        return sourceResultFuture;
    }


}
