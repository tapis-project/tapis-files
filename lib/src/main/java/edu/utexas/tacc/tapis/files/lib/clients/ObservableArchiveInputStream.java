package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class ObservableArchiveInputStream extends FilterInputStream {

    public static interface Observer {
        void file(String name, long size, String digest);
        void actualBytesRead(long actualBytesRead);
    }

    private final static int BUFFER_SIZE = 50000;
    private boolean finished = false;

    private final static int READ_BUFFER_SIZE = 500;
    private final TarArchiveInputStream tarArchiveInputStream;
    private final TarArchiveOutputStream tarArchiveOutputStream;
    private final ByteArrayOutputStream byteArrayOutputStream;
    TarArchiveEntry currentTarEntry = null;
    int readPosition = 0;
    int totalRead = 0;
    Logger log = LoggerFactory.getLogger(ObservableArchiveInputStream.class);
    private final List<Observer> observerList = new ArrayList<>();
    private final MessageDigest md;
    public ObservableArchiveInputStream(InputStream in, boolean useCompression) throws IOException {
        this(in, null, useCompression);
    }

    public ObservableArchiveInputStream(InputStream in, MessageDigest md, boolean useCompression) throws IOException {
        super(in);
        byteArrayOutputStream = new ByteArrayOutputStream();
        if(useCompression) {
            tarArchiveInputStream = new TarArchiveInputStream(new GZIPInputStream(in));
            tarArchiveOutputStream = new TarArchiveOutputStream(new GZIPOutputStream(byteArrayOutputStream));
        } else {
            tarArchiveInputStream = new TarArchiveInputStream(in);
            tarArchiveOutputStream = new TarArchiveOutputStream(byteArrayOutputStream);
        }
        // store the MessageDigest (may be null)
        this.md = md;
    }

    @Override
    public int read() throws IOException {
        try {
            if ((this.finished) && (bytesLeftInReadBuffer() == 0)) {
                notifyArchiveSize();
                return -1;
            }

            if (bytesLeftInReadBuffer() <= 0) {
                byteArrayOutputStream.reset();
                readPosition = 0;
                if (fillReadBuffer(READ_BUFFER_SIZE) == -1) {
                    notifyArchiveSize();
                    return -1;
                }
            }

            int readByte = (0x000000FF) & byteArrayOutputStream.toByteArray()[readPosition];
            totalRead++;
            readPosition++;
            return readByte;
        } catch (Throwable th) {
            log.error("Caught throwable in method read", th);
            throw new RuntimeException("Error in observableStream read: ", th);
        }
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
        try {
            return read(b, 0, b.length);
        } catch (Throwable th) {
            log.error("Caught throwable in method read", th);
            throw new RuntimeException("Error in observableStream read: ", th);
        }
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        try {
            if (len == 0) {
                return 0;
            }

            if ((this.finished) && (bytesLeftInReadBuffer() == 0)) {
                notifyArchiveSize();
                return -1;
            }

            int bytesRead = 0;

            while(bytesRead < len) {
                if (bytesLeftInReadBuffer() <= 0) {
                    byteArrayOutputStream.reset();
                    readPosition = 0;
                    if (fillReadBuffer(READ_BUFFER_SIZE) == -1) {
                        return bytesRead;
                    }
                }

                int copyLength = Math.min(len - bytesRead, bytesLeftInReadBuffer());

                System.arraycopy(byteArrayOutputStream.toByteArray(), readPosition, b, off + bytesRead, copyLength);
//                int readByte = (0x000000FF) & byteArrayOutputStream.toByteArray()[readPosition];
                totalRead += copyLength;
                readPosition += copyLength;
                bytesRead += copyLength;
/*
                if (readResult == -1) {
                    // if we got a -1, and we have nothing buffered up, we must be at the end.
                    if (bytesRead == 0) {
                        return -1;
                    }
                    break;
                }
                b[off + bytesRead] = (byte) readResult;
 */
            }

            log.info("read with offset: " + "byte[] length: " + b.length + " off: " + off + "len: " + len + " read:" + bytesRead + " totalRead: " + totalRead);

            return bytesRead;
        } catch (Throwable th) {
            log.error("Caught throwable in method read", th);
            throw new RuntimeException("Error in observableStream read: ", th);
        }
    }

    /*
    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        try {
            int bytesRead = 0;

            if (len == 0) {
                return 0;
            }

            for (bytesRead = 0; bytesRead < len; bytesRead++) {
                int readResult = read();
                if (readResult == -1) {
                    // if we got a -1, and we have nothing buffered up, we must be at the end.
                    if (bytesRead == 0) {
                        return -1;
                    }
                    break;
                }
                b[off + bytesRead] = (byte) readResult;
            }

            log.info("read with offset: " + "byte[] length: " + b.length + " off: " + off + "len: " + len + " read:" + bytesRead + " totalRead: " + totalRead);

            return bytesRead;
        } catch (Throwable th) {
            log.error("Caught throwable in method read", th);
            throw new RuntimeException("Error in observableStream read: ", th);
        }
    }
*/
    private int bytesLeftInReadBuffer() {
        return byteArrayOutputStream.size() - readPosition;
    }

    @Override
    public int available() throws IOException {
        // available is the bytes in the in stream plus whats in the buffer
        if((!finished) && (bytesLeftInReadBuffer() == 0)) {
            byteArrayOutputStream.reset();
            readPosition = 0;
            fillReadBuffer(BUFFER_SIZE);
        }

        int available = bytesLeftInReadBuffer();
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

        return bytesLeftInReadBuffer();
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
                tarArchiveOutputStream.flush();
                tarArchiveOutputStream.close();
                finished = true;
            }
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

    private void notifyArchiveSize() {
        observerList.stream().forEach(observer -> {
            try {
                observer.actualBytesRead(totalRead);
            } catch (Throwable th) {
                // TODO AXFER:  log this error - add longging etc remove println
                th.printStackTrace();
            }
        });
    }

    public void addObserver(Observer observer) {
        observerList.add(observer);
    }
}
