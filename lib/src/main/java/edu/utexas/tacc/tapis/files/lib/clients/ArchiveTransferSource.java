package edu.utexas.tacc.tapis.files.lib.clients;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Set;

public interface ArchiveTransferSource {
    ArchiveInputPipe getArchiveStream(@NotNull String srcBasePath,
                                      @NotNull Set<String> relativePaths,
                                      ArchiveTransferProvider archiveTransferProvider) throws IOException;
}
