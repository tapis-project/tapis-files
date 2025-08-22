package edu.utexas.tacc.tapis.files.lib.clients;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.concurrent.Future;

public interface ArchiveTransferDestination {
    ArchiveTransferResult writeArchive(@NotNull String basePath,
                                       @NotNull InputStream archiveInputStream,
                                       ArchiveTransferProvider archiveTransferProvider,
                                       Future<SSHCommandResult> sourceResultFuture) throws IOException;
}
