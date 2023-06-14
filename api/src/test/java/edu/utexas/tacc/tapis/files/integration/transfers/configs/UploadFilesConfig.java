package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.nio.file.Path;

public class UploadFilesConfig {
    private String uploadSystem;
    private Path uploadPath;
    private int count;
    private long size;

    public String getUploadSystem() {
        return uploadSystem;
    }

    public Path getUploadPath() {
        return uploadPath;
    }

    public int getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }
}
