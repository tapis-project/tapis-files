package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveTransferProvider;
import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveTransferResult;
import edu.utexas.tacc.tapis.files.lib.models.SSHCommandResult;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Future;

public interface ArchiveTransferDestination {
    default ArchiveTransferResult writeArchive(@NotNull String basePath,
                                       @NotNull InputStream archiveInputStream,
                                       ArchiveTransferProvider archiveTransferProvider) throws IOException {
        return this.writeArchive(basePath, archiveInputStream, archiveTransferProvider, null);
    }

    ArchiveTransferResult writeArchive(@NotNull String basePath,
                                       @NotNull InputStream archiveInputStream,
                                       ArchiveTransferProvider archiveTransferProvider,
                                       Future<SSHCommandResult> sourceResultFuture) throws IOException;
}
