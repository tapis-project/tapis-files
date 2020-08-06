package edu.utexas.tacc.tapis.files.lib.transfers;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.function.Supplier;

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
            events.onNext(this.totalBytesRead);
        } else {
            events.onComplete();
        }
        return bytesRead;
    }




}
