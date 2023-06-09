package edu.utexas.tacc.tapis.files.integration.transfers.configs;

public class UploadFilesConfig {
    private String uploadSystem;
    private String uploadPath;
    private int count;
    private long size;


    public String getUploadSystem() {
        return uploadSystem;
    }

    public String getUploadPath() {
        return uploadPath;
    }

    public int getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }
}
