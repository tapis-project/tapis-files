package edu.utexas.tacc.tapis.files.integration.transfers;

import edu.utexas.tacc.tapis.files.test.RandomByteInputStream;
import edu.utexas.tacc.tapis.files.test.TestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public class RandomStreamWriter implements Runnable {
    boolean completedSuccessfully = false;
    String digest = null;

    private final int bytesToWrite;
    private final boolean alphaNumericOnly;
    private PipedOutputStream outputStream;
    private String messageDigestAlgorithm;

    public RandomStreamWriter(int bytesToWrite, boolean alphaNumericOnly, String messageDigestAlgorithm) {
        this.bytesToWrite = bytesToWrite;
        this.alphaNumericOnly = alphaNumericOnly;
        this.messageDigestAlgorithm = messageDigestAlgorithm;
    }

    public InputStream initInputStream() throws IOException {
        outputStream = new PipedOutputStream();
        return new PipedInputStream(outputStream);
    }

    @Override
    public void run() {
        try {
            RandomByteInputStream randomInputStream = new RandomByteInputStream(bytesToWrite,
                    RandomByteInputStream.SizeUnit.BYTES, alphaNumericOnly);
            DigestInputStream digestInputStream = new DigestInputStream(randomInputStream, MessageDigest.getInstance(messageDigestAlgorithm));
            digestInputStream.transferTo(outputStream);
            digest = TestUtils.hashAsHex(digestInputStream.getMessageDigest().digest());
            completedSuccessfully = true;
        } catch (Throwable th) {
            throw new RuntimeException(th);
        } finally {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getDigest() {
        return digest;
    }

    public boolean isCompletedSuccessfully() {
        return completedSuccessfully;
    }
}
