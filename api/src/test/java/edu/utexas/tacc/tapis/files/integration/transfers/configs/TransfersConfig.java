package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import org.junit.Assert;

import java.nio.file.Path;

public class TransfersConfig {
    private String sourceProtocol;
    private String sourceSystem;
    private Path sourcePath;
    private String destinationProtocol;
    private String destinationSystem;
    private Path destinationPath;
    // default to waiting 10 seconds

    public String getSourceSystem() {
        return sourceSystem;
    }

    public Path getSourcePath() {
        return sourcePath;
    }

    public String getDestinationSystem() {
        return destinationSystem;
    }

    public Path getDestinationPath() {
        return destinationPath;
    }

    public String getSourceProtocol() {
        return sourceProtocol;
    }

    public String getDestinationProtocol() {
        return destinationProtocol;
    }

    public String getTapisSourcePath(Path relativeFilePath) {
        Assert.assertFalse(relativeFilePath.isAbsolute());
        Path fullPath = getSourcePath().resolve(relativeFilePath);
        return getSourceProtocol() + "://" + getSourceSystem() + "/" + fullPath.toString();
    }

    public String getTapisDestinationPath(Path relativeFilePath) {
        Assert.assertFalse(relativeFilePath.isAbsolute());
        Path fullPath = getDestinationPath().resolve(relativeFilePath);
        return getDestinationProtocol() + "://" + getDestinationSystem() + "/" + fullPath.toString();
    }
}
