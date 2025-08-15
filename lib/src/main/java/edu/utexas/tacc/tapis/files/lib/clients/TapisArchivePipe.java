package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.concurrent.Future;

public class TapisArchivePipe extends Pipe {
    private final Pipe pipe;
    private Future<SSHCommandResult> sourceResultFuture;

    public static TapisArchivePipe open() throws IOException {
        Pipe pipe = Pipe.open();
        return new TapisArchivePipe(pipe);
    }

    TapisArchivePipe(Pipe pipe) {
        this.pipe = pipe;
    }

    @Override
    public SourceChannel source() {
        return pipe.source();
    }

    @Override
    public SinkChannel sink() {
        return pipe.sink();
    }

    public void setSourceResult(Future<SSHCommandResult> sourceResultFuture) {
        this.sourceResultFuture = sourceResultFuture;
    }

    public Future<SSHCommandResult> getSourceResultFuture() {
        return sourceResultFuture;
    }


}
