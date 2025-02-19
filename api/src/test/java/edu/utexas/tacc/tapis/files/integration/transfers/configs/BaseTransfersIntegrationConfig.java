package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.util.Collections;
import java.util.List;

public class BaseTransfersIntegrationConfig {
    List<UploadFilesConfig> uploadFiles;
    List<CleanupConfig> cleanup;

    public List<UploadFilesConfig> getUploadFiles() {
        return uploadFiles == null ? Collections.<UploadFilesConfig>emptyList() : uploadFiles;
    }

    public List<CleanupConfig> getCleanup() {
        return cleanup == null ? Collections.<CleanupConfig>emptyList() : cleanup;
    }
}
