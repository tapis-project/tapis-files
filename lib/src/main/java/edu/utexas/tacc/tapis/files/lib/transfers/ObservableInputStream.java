package edu.utexas.tacc.tapis.files.lib.transfers;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * This class wraps a native InputStream and sends events on each read as to how many bytes
 * were read. An EmitterProcessor is used to generate the events, which can be subscribed to
 * during the actual file transfer
 */
public class ObservableInputStream extends FilterInputStream {

    private final EmitterProcessor<Long> events = EmitterProcessor.create();
    public ObservableInputStream(InputStream in) {
        super(in);

    }
    private long totalBytesRead = 0L;

    @Override
    public int read() throws IOException {
        int b = super.read();
        return (int) updateProgress(b);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return (int) updateProgress(super.read(b, off, len));
    }

    public Flux<Long> getEventStream() {
        return events;
    }

    private long updateProgress(long bytesRead) {
        if (bytesRead != -1) {
            this.totalBytesRead += bytesRead;
            // Send event to whoever is listening
            events.onNext(this.totalBytesRead);
        } else {
            // If no more bytes, kill off the emitter also
            events.onComplete();
        }
        return bytesRead;
    }
}
