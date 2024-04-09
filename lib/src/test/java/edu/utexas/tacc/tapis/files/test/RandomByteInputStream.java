package edu.utexas.tacc.tapis.files.test;

import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.NoSuchAlgorithmException;

public class RandomByteInputStream extends InputStream {
//    private final MessageDigest digest;

    private int bytesRead = 0;
    private final int bytesAvailable;
    private final int maxChunkSize;
    private final boolean alphaNumericOnly;
    private PipedOutputStream outputStream;
    public static int DEFAULT_CHUNK_SIZE = 2048;

    public static enum SizeUnit {
        BYTES,
        KILOBYTES,
        MEGABYTES;

        private static int MULTIPLIER = 1024;

        int inBytes(int units) {
            int sizeInBytes = 0;

            switch(this) {
                case BYTES:
                    sizeInBytes = units;
                    break;

                case KILOBYTES:
                    sizeInBytes = units * MULTIPLIER;
                    break;

                case MEGABYTES:
                    sizeInBytes = units * MULTIPLIER * MULTIPLIER;
                    break;

                default:
                    throw new RuntimeException("Unknown size unit");
            }

            return sizeInBytes;
        }
    }

    public RandomByteInputStream(int size, SizeUnit sizeUnit, boolean alphaNumericOnly) throws NoSuchAlgorithmException {
        this.bytesAvailable = sizeUnit.inBytes(size);
        this.alphaNumericOnly = alphaNumericOnly;
        this.maxChunkSize = DEFAULT_CHUNK_SIZE;
    }

    public RandomByteInputStream(int maxChunkSize, int size, SizeUnit sizeUnit, boolean alphaNumericOnly) throws NoSuchAlgorithmException {
        this.bytesAvailable = sizeUnit.inBytes(size);
        this.alphaNumericOnly = alphaNumericOnly;
        this.maxChunkSize = maxChunkSize;
    }

    public InputStream initInputStream() throws IOException {
        outputStream = new PipedOutputStream();
        return new PipedInputStream(outputStream);
    }

    public int available() {
        return bytesAvailable - bytesRead;
    }

    @Override
    public int read() throws IOException {
        if(available() <= 0) {
            return -1;
        }
        byte [] bytes = getRandomBytes(1);
        bytesRead++;
        return bytes[0];
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        if(len == 0) {
            return 0;
        } else if(available() <= 0) {
            return -1;
        }

        int capacity = b.length - off;
        if(capacity <= 0) {
            throw new RuntimeException("byte array capacity (begining at offset) must be greater than 0");
        }

        // max bytes requested cannot exceed what fits in the byte array.
        int bytesToRead = Math.min(capacity, len);

        // We will never read more than  maxChunkSize
        bytesToRead = Math.min(bytesToRead, maxChunkSize);

        // we must not return more bytes than avaliable for this stream (i.e. respect
        // the end of file).
        bytesToRead = Math.min(bytesToRead, available());

        byte [] bytes = getRandomBytes(bytesToRead);
        for(int i = 0;i < bytes.length;i++) {
            b[off + i] = bytes[i];
        }
        bytesRead += bytes.length;
        return bytes.length;
    }

    private byte[] getRandomBytes(int count) {
        byte[] bytes = null;
        if(alphaNumericOnly) {
            bytes = RandomStringUtils.randomAlphanumeric(count).getBytes();
        } else {
            bytes = RandomStringUtils.random(count).getBytes();
        }

        return  bytes;
    }

}
