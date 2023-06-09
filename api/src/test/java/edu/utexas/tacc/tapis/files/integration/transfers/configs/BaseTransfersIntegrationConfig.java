package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.util.List;

public class BaseTransfersIntegrationConfig {
//    List<CreateLocalFilesConfig> createLocalFilesConfig;
    List<UploadFilesConfig> uploadFilesConfig;

//    public List<CreateLocalFilesConfig> getCreateLocalFilesConfig() {
//        return createLocalFilesConfig;
//    }

    public List<UploadFilesConfig> getUploadFilesConfig() {
        return uploadFilesConfig;
    }
}
