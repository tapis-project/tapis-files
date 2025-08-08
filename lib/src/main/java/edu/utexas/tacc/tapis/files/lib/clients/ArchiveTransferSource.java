package edu.utexas.tacc.tapis.files.lib.clients;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Future;

public interface ArchiveTransferSource {
    TapisArchiveInputStream getArchiveStream(@NotNull String srcBasePath,
                                              @NotNull Set<String> relativePaths) throws IOException;
}
