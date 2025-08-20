package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;


public class ObservableTapisArchiveInputStream extends FilterInputStream {

    public static interface Observer {
        void file(String name, long size, String digest);
        void total(long archiveBytesRead, long fileBytesRead);
    }

    private final static int BUFFER_SIZE = 10000;
    private boolean finished = false;

    private final static int READ_BUFFER_SIZE = 500;
    private final TarArchiveInputStream tarArchiveInputStream;
    private final TarArchiveOutputStream tarArchiveOutputStream;
    private final ByteArrayOutputStream byteArrayOutputStream;
    TarArchiveEntry currentTarEntry = null;
    int readPosition = 0;
    int totalRead = 0;
    Logger log = LoggerFactory.getLogger(ObservableTapisArchiveInputStream.class);
    private final List<Observer> observerList = new ArrayList<>();
    private final MessageDigest md;
    /*
        private long archiveBytesRead = 0;
        private long fileBytesRead = 0;
        private final ByteBuffer readBuffer;

    */
    public ObservableTapisArchiveInputStream(InputStream in) throws IOException {
        this(in, null);
    }

    public ObservableTapisArchiveInputStream(InputStream in, MessageDigest md) throws IOException {
        super(in);
        byteArrayOutputStream = new ByteArrayOutputStream();
        tarArchiveInputStream = new TarArchiveInputStream(in);
        tarArchiveOutputStream = new TarArchiveOutputStream(byteArrayOutputStream);
        // store the MessageDigest (may be null)
        this.md = md;

        /*
        readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        readBuffer.flip();

         */
    }

    @Override
    public int read() throws IOException {
        /*
        // if there are no bytes in the read buffer,
        // fill it.
        while(!readBuffer.hasRemaining()) {
            if(fillBuffer() == -1) {
                return -1;
            }
        }

        // return the next byte in the read buffer.
        return readBuffer.get();
         */

        if(byteLeftInReadBuffer() <= 0) {
            byteArrayOutputStream.reset();
            readPosition = 0;
            if(fillReadBuffer(READ_BUFFER_SIZE) == -1) {
                return -1;
            }
        }

        byte readByte =  byteArrayOutputStream.toByteArray()[readPosition];
        readPosition++;
        return readByte;
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
        /*
        while(!readBuffer.hasRemaining()) {
            if(fillBuffer() == -1) {
                return -1;
            }
        }

        int currentPos = readBuffer.position();
        readBuffer.get(b);
        return readBuffer.position() - currentPos;
         */
        return read(b, 0, b.length);
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        /*
        while(!readBuffer.hasRemaining()) {
            if(fillBuffer() == -1) {
                return -1;
            }
        }

        int bytesToRead = Math.min(readBuffer.limit() - readBuffer.position(), len);
        int currentPos = readBuffer.position();
        readBuffer.get(b, off, bytesToRead);
        return readBuffer.position() - currentPos;
         */
        if(len == 0) {
            return 0;
        }

        int bytesRead = 0;
        for(bytesRead = 0;bytesRead < len;bytesRead++) {
            int readResult  = read();
            if(readResult == -1) {
                // if we got a -1, and we have nothing buffered up, we must be at the end.
                if(bytesRead == 0) {
                    return -1;
                }
                break;
            }
            b[off + bytesRead] = (byte) readResult;
        }

        totalRead += bytesRead;
        log.error("read with offset: " + "byte[] length: " + b.length + " off: " + off + "len: " + len + " read:" + bytesRead + " totalRead: " + totalRead);

        return  bytesRead;
    }

    private int byteLeftInReadBuffer() {
        return byteArrayOutputStream.size() - readPosition;
    }

    @Override
    public int available() throws IOException {
        // available is the bytes in the in stream plus whats in the buffer
//        return in.available() + (readBuffer.limit() - readBuffer.position());
        if((!finished) && (byteLeftInReadBuffer() == 0)) {
            byteArrayOutputStream.reset();
            readPosition = 0;
            fillReadBuffer(BUFFER_SIZE);
        }

        int available = byteLeftInReadBuffer();
        return available;
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

    public int fillReadBuffer(int bytesToRead) throws IOException {
        if(finished) {
            return -1;
        }

        goToNextTarArchiveEntry();

        byte[] fileBytes = new byte[READ_BUFFER_SIZE];
        while((byteArrayOutputStream.size() < bytesToRead) && (currentTarEntry != null)) {
            int bytesRead = tarArchiveInputStream.read(fileBytes);
            if(bytesRead == -1) {
                tarArchiveOutputStream.closeArchiveEntry();
                notifyFile();
                currentTarEntry = null;
                goToNextTarArchiveEntry();
            } else {
                tarArchiveOutputStream.write(fileBytes, 0, bytesRead);
                updateMd(fileBytes, 0, bytesRead);
            }
        }

        return byteLeftInReadBuffer();
    }

    public void goToNextTarArchiveEntry() throws IOException {
        // see if we are in the middle of reading a tar entry
        if (currentTarEntry == null) {
            currentTarEntry = tarArchiveInputStream.getNextTarEntry();
            if (currentTarEntry != null) {
                tarArchiveOutputStream.putArchiveEntry(currentTarEntry);
            } else {
                // finish the archive, and get the remaining bytes
                tarArchiveOutputStream.finish();
                tarArchiveOutputStream.close();
                finished = true;
            }
        }
    }
/*
    // returns bytes read, or -1 for EOF
    private long fillBuffer() throws IOException {
        if(finished) {
            return -1;
        }

        // if we are not currently working on a tar entry, get the next one
        if(currentTarEntry == null) {
            currentTarEntry = tarArchiveInputStream.getNextTarEntry();
            if(currentTarEntry != null) {
                tarArchiveOutputStream.putArchiveEntry(currentTarEntry);
                readBuffer.compact();
                byte[] fileBytes = byteArrayOutputStream.toByteArray();
                readBuffer.put(fileBytes);
                readBuffer.flip();
                byteArrayOutputStream.reset();
            } else {
                tarArchiveOutputStream.finish();
                finished = true;
            }
            resetMd();
        }

        // TODO read after no more entries?  Is that a thing?

        int bytesRead = -1;

        // if we find a new entry process it (or continue to process it), but if not we are at the end.
        if (currentTarEntry != null) {
            readBuffer.compact();
            bytesRead = processTarEntry(readBuffer.limit() - readBuffer.position() );
            if(bytesRead != -1) {
                byte[] fileBytes = byteArrayOutputStream.toByteArray();
                readBuffer.put(fileBytes);
            } else {
                notifyFile();
                System.out.println("File Info -- Name: " + currentTarEntry.getName() +
                        " Size: " + currentTarEntry.getSize() +
                        " SHA: " + getMd());
                tarArchiveOutputStream.closeArchiveEntry();
                currentTarEntry = null;
            }
            readBuffer.flip();
            byteArrayOutputStream.reset();
        }

        return bytesRead;
    }

    private int processTarEntry(int maxBytesToRead) throws IOException {
        byte[] bytes = new byte[maxBytesToRead];
        int bytesRead = tarArchiveInputStream.read(bytes);
        byte[] returnedBytes = Arrays.copyOf(bytes, bytesRead);

        if(bytesRead != -1) {
            tarArchiveOutputStream.write(returnedBytes);
            updateMd(returnedBytes);
        }

        return bytesRead;
    }
*/
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

    private void updateMd(byte[] bytes, int off, int len) {
        if(md != null) {
            md.update(bytes, off, len);
        }
    }

    private String getMd() {
        if(md != null) {
            return hashAsHex(md.digest());
        } else {
            return "Not Calculated";
        }
    }

    private void notifyFile() {
        String name = currentTarEntry.getName();
        long size = currentTarEntry.getSize();
        String digest = getMd();
        observerList.stream().forEach(observer -> {
            try {
                observer.file(name, size, digest);
            } catch (Throwable th) {
                // TODO AXFER:  log this error - add longging etc remove println
                th.printStackTrace();
            }
        });
    }
/*
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
   */

    public void addObserver(Observer observer) {
        observerList.add(observer);
    }
}
