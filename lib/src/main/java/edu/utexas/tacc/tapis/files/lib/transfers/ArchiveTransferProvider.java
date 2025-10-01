package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.UnrecoverableTransferException;
import edu.utexas.tacc.tapis.files.lib.models.SSHCommandResult;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
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
import java.util.concurrent.Future;

public class ArchiveTransferProvider {
    public enum ArchiveType {
        TAR,
        TAR_GZIP,
        TO_TAR_ARCHIVE,
        TO_GZIP_ARCHIVE,
        FROM_TAR_ARCHIVE,
        FROM_GZIP_ARCHIVE
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
            case TAR, TO_TAR_ARCHIVE, FROM_TAR_ARCHIVE -> new TarArchiveInputStream(in);

            case TAR_GZIP, TO_GZIP_ARCHIVE, FROM_GZIP_ARCHIVE -> new TarArchiveInputStream(new GzipCompressorInputStream(in));
        };

        return archiveInputStream;
    }

    public ArchiveOutputStream getArchiveOutputStream(OutputStream out) throws IOException {
        ArchiveOutputStream archiveOutputStream = switch (archiveType) {
            case TAR, TO_TAR_ARCHIVE, FROM_TAR_ARCHIVE -> new TarArchiveOutputStream(out);

            case TAR_GZIP, TO_GZIP_ARCHIVE, FROM_GZIP_ARCHIVE -> new TarArchiveOutputStream(new GzipCompressorOutputStream(out));
        };

        return archiveOutputStream;
    }

    public String getArchiveCommand(String srcAbsBasePath) {
        final String opName = "getArchiveCommand";

        String command = switch (archiveType) {
            case TAR, TO_TAR_ARCHIVE -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(srcAbsBasePath);
                commandBuilder.append("' --verbatim-files-from -cT- ");
                yield commandBuilder.toString();
            }

            case TAR_GZIP, TO_GZIP_ARCHIVE -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(srcAbsBasePath);
                commandBuilder.append("' --verbatim-files-from -czT- ");
                yield commandBuilder.toString();
            }

            default -> {
                String msg = LibUtils.getMsg("FILES_XFER_INVALID_PARAMETER", opName, "archiveType", archiveType);
                throw new UnrecoverableTransferException(msg);
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
            case TAR, TO_TAR_ARCHIVE, FROM_TAR_ARCHIVE -> {
                StringBuilder commandBuilder = new StringBuilder();
                commandBuilder.append("tar -C '");
                commandBuilder.append(dstAbsBasePath);
                commandBuilder.append("' -x");
                yield commandBuilder.toString();
            }

            case TAR_GZIP, TO_GZIP_ARCHIVE, FROM_GZIP_ARCHIVE -> {
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

    public ArchiveTransferResult getArchiveTransferResult(Future<SSHCommandResult> sourceCommandResult,
                                                          Future<SSHCommandResult> destinationCommandResult) {
        final String opName = "getArchiveTransferResult";
        ArchiveTransferResult result = switch (archiveType) {
            case TAR, TAR_GZIP -> new FullArchiveTransferResult(sourceCommandResult, destinationCommandResult);
            case FROM_TAR_ARCHIVE, FROM_GZIP_ARCHIVE -> new FromArchiveTransferResult(destinationCommandResult);

            default -> {
                String msg = LibUtils.getMsg("FILES_XFER_INVALID_PARAMETER", opName, "archiveType", archiveType);
                throw new UnrecoverableTransferException(msg);
            }
        };

        return result;
    }
}
