package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.nio.file.Path;

public class UploadFilesConfig {
    private String uploadSystem;
    private Path uploadPath;
    private int count;
    private int size;
    private String filePrefix;

    public String getUploadSystem() {
        return uploadSystem;
    }

    public Path getUploadPath() {
        return uploadPath;
    }

    public int getCount() {
        return count;
    }

    public int getSize() {
        return size;
    }

    public String getFilePrefix() {
        return filePrefix;
    }
}
