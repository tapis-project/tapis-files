package edu.utexas.tacc.tapis.files.lib.clients;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;

public class ArchiveTransferProvider {
    public enum ArchiveType {
        TAR,
        TAR_GZIP
    }

    private final ArchiveType archiveType;
    private final MessageDigest messageDigest;

    public ArchiveTransferProvider(ArchiveType archiveType) {
        this(archiveType, null);
    }

    public ArchiveTransferProvider(ArchiveType archiveType, MessageDigest messageDigest) {
        this.archiveType = archiveType;
        this.messageDigest = messageDigest;
    }

    public ArchiveInputStream getArchiveInputStream(InputStream in) throws IOException {
        ArchiveInputStream archiveInputStream = switch (archiveType) {
            case TAR -> new TarArchiveInputStream(in);

            case TAR_GZIP -> new TarArchiveInputStream(new GzipCompressorInputStream(in));
        };

        return archiveInputStream;
    }

    public ArchiveOutputStream getArchiveOutputStream(OutputStream out) throws IOException {
        ArchiveOutputStream archiveOutputStream = switch (archiveType) {
            case TAR -> new TarArchiveOutputStream(out);

            case TAR_GZIP -> new TarArchiveOutputStream(new GzipCompressorOutputStream(out));
        };

        return archiveOutputStream;
    }

    public String getArchiveCommand(String srcAbsBasePath) {

        String command = switch (archiveType) {
            case TAR -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(srcAbsBasePath);
                commandBuilder.append("' -cT- ");
                yield commandBuilder.toString();
            }

            case TAR_GZIP -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(srcAbsBasePath);
                commandBuilder.append("' -czT- ");
                yield commandBuilder.toString();
            }
        };

        return command;
    }

    public String getUnarchiveCommand(String dstAbsBasePath) {
        // possibly add ignore failed for optional? could mask other errors though
        // commandBuilder.append("' --ignore-failed-read -cT- ");

        // add -h to follow links
        // commandBuilder.append("' -hcT- ");

        String command = switch (archiveType) {
            case TAR -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(dstAbsBasePath);
                commandBuilder.append("' -x");
                yield commandBuilder.toString();
            }

            case TAR_GZIP -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(dstAbsBasePath);
                commandBuilder.append("' -xz");
                yield commandBuilder.toString();
            }
        };

        return command;
    }

    public MessageDigest getMessageDigest() {
        return messageDigest;
    }
}
