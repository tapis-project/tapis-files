package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.nio.file.Path;

public class TransfersConfig {
    private String sourceProtocol;
    private String sourceSystem;
    private String sourcePath;
    private String destinationProtocol;
    private String destinationSystem;
    private String destinationPath;

    public String getSourceSystem() {
        return sourceSystem;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getDestinationSystem() {
        return destinationSystem;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

    public String getSourceProtocol() {
        return sourceProtocol;
    }

    public String getDestinationProtocol() {
        return destinationProtocol;
    }

    public String getTapisSourcePath(String relativeFilePath) {
        Path sourcePath = Path.of(getSourceSystem(), getSourcePath(), relativeFilePath);
        return getSourceProtocol() + "://" + sourcePath.toString();
    }

    public String getTapisDestinationPath(String relativeFilePath) {
        Path destinationPath = Path.of(getDestinationSystem(), getDestinationPath(), relativeFilePath);
        return getDestinationProtocol() + "://" + destinationPath.toString();
    }

}
