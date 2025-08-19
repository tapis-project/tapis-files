package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;


public class ObservableTapisArchiveInputStream extends FilterInputStream {
    public static interface Observer {
        void file(String path, String name, long size, String digest);
        void total(long archiveBytesRead, long fileBytesRead);
    }

    private final static int BUFFER_SIZE = 10000;
    private final static int FILE_BYTES_MAX = 10000;
    private final TarArchiveInputStream tarArchiveInputStream;
    private final TarArchiveOutputStream tarArchiveOutputStream;
    private final ByteBuffer readBuffer;
    private final Pipe pipe;
    private final MessageDigest md;
    private boolean finished = false;
    private long archiveBytesRead = 0;
    private long fileBytesRead = 0;
    private final List<Observer> observerList = new ArrayList<>();

    public ObservableTapisArchiveInputStream(ReadableByteChannel readableByteChannel) throws IOException {
        this(Channels.newInputStream(readableByteChannel), null);
    }

    public ObservableTapisArchiveInputStream(ReadableByteChannel readableByteChannel, MessageDigest md) throws IOException {
        this(Channels.newInputStream(readableByteChannel), md);
    }

    public ObservableTapisArchiveInputStream(InputStream in) throws IOException {
        this(in, null);
    }

    public ObservableTapisArchiveInputStream(InputStream in, MessageDigest md) throws IOException {
        super(in);
        tarArchiveInputStream = new TarArchiveInputStream(in);


        // setup the pipe that allows us to inspect contents of the tar archive.  The
        // read side is connected to the input tar archive input stream wrapping the
        // InputStream that gets passed in. The write side (sink) is connected to the
        // read buffer.
        pipe = Pipe.open();
        pipe.source().configureBlocking(false);
        tarArchiveOutputStream = new TarArchiveOutputStream(Channels.newOutputStream(pipe.sink()));

        // setup the buffer used for read() calls
        readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        readBuffer.flip();

        // store the MessageDigest (may be null)
        this.md = md;
    }

    @Override
    public int read() throws IOException {
        // if there are no bytes in the read buffer,
        // fill it.
        while(!readBuffer.hasRemaining()) {
            if(fillBuffer() == -1) {
                return -1;
            }
        }

        // return the next byte in the read buffer.
        return readBuffer.get();
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
        while(!readBuffer.hasRemaining()) {
            if(fillBuffer() == -1) {
                return -1;
            }
        }

        int currentPos = readBuffer.position();
        readBuffer.get(b);
        return readBuffer.position() - currentPos;
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        while(!readBuffer.hasRemaining()) {
            if(fillBuffer() == -1) {
                return -1;
            }
        }

        int bytesToRead = Math.min(readBuffer.limit() - readBuffer.position(), len);
        int currentPos = readBuffer.position();
        readBuffer.get(b, off, bytesToRead);
        return readBuffer.position() - currentPos;
    }

    @Override
    public int available() throws IOException {
        // available is the bytes in the in stream plus whats in the buffer
        return in.available() + (readBuffer.limit() - readBuffer.position());
    }

    @Override
    public void mark(int readlimit) {
        super.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        super.reset();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public long skip(long n) throws IOException {
        return super.skip(n);
    }

    // returns bytes read, or -1 for EOF
    private  int fillBuffer() throws IOException {
        if(finished) {
            return -1;
        }

        TarArchiveEntry entry = tarArchiveInputStream.getNextTarEntry();
        if (entry != null) {
            processTarEntry(entry);
        } else {
            // I'm not sure if it's possible to not get a TarArchiveEntry,
            // but still have bytes to read, but handle it just in case.
            readBuffer.compact();
            int bytesRead = pipe.source().read(readBuffer);
            readBuffer.flip();
            if((bytesRead == 0) && (!readBuffer.hasRemaining())) {
                // no more archive entries, and nothing left in pipe, so
                // end the stream;
                tarArchiveOutputStream.finish();
                finished = true;
                notifyTotal();
                return -1;
            }
        }

        readBuffer.compact();
        int bytesRead = pipe.source().read(readBuffer);
        readBuffer.flip();

        archiveBytesRead += bytesRead;
        return bytesRead;
    }

    private void processTarEntry(TarArchiveEntry entry) throws IOException {
        tarArchiveOutputStream.putArchiveEntry(entry);
        writeFileBytes();
        tarArchiveOutputStream.closeArchiveEntry();
        notifyFile(entry.getPath() == null ? "null" : entry.getPath().toString(), entry.getName(), entry.getSize(), getMd());
        System.out.println("File Info -- Name: " + entry.getName() +
                " Size: " + entry.getSize() +
                " SHA: " + getMd());
    }

    private void writeFileBytes() throws IOException {
        ReadableByteChannel inChannel = Channels.newChannel(tarArchiveInputStream);
        WritableByteChannel outChannel = Channels.newChannel(tarArchiveOutputStream);
        ByteBuffer fileByteBuffer = ByteBuffer.allocate(FILE_BYTES_MAX);
        resetMd();
        int bytesRead = 0;
        while ((bytesRead = inChannel.read(fileByteBuffer)) != -1) {
            fileByteBuffer.flip();
            updateMd(fileByteBuffer);
            this.fileBytesRead += bytesRead;
            while (fileByteBuffer.hasRemaining()) {
                outChannel.write(fileByteBuffer);
            }
            fileByteBuffer.flip();
        }
    }

    private String hashAsHex(byte[] hashBytes) {
        StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
        for (int i = 0; i < hashBytes.length; i++) {
            String hex = Integer.toHexString(0xff & hashBytes[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return "sha256:" + hexString.toString();
    }

    private void resetMd() {
        if(md != null) {
            md.reset();
        }
    }

    private void updateMd(ByteBuffer mdByteBuffer) {
        if(md != null) {
            mdByteBuffer.mark();
            md.update(mdByteBuffer);
            mdByteBuffer.reset();
        }
    }
    private String getMd() {
        if(md != null) {
            return hashAsHex(md.digest());
        } else {
            return "Not Calculated";
        }
    }

    private void notifyFile(String path, String name, long size, String digest) {
        observerList.stream().forEach(observer -> {
            observer.file(path, name, size, digest);
        });
    }

    private void notifyTotal() {
        observerList.stream().forEach(observer -> {
            observer.total(archiveBytesRead, fileBytesRead);
        });
    }


    public long getFileBytesRead() {
        return fileBytesRead;
    }

    public long getArchiveBytesRead() {
        return archiveBytesRead;
    }

    public void addObserver(Observer observer) {
        observerList.add(observer);
    }
}
