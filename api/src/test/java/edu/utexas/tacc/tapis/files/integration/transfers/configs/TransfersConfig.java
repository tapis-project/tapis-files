package edu.utexas.tacc.tapis.files.integration.transfers.configs;


import org.testng.Assert;

import java.nio.file.Path;

/**
 * the source can either be a tapis transfer with a directory - tests will create a bunch of files
 * and upload them to that tapis directory - or if it's a specific file (usually for http(s) but it
 * could be a single known file for tapis also), it must include the sourceFileName and sourceSHA
 */
public class TransfersConfig {
    // used only if sourceFileName is present (transferring a single known file)
    private String sourceSHA;
    private String sourceProtocol;
    private String sourceSystem;
    private Path sourcePath;
    // if sourceFileName is present must provide a sourceSHA (transferring a single known file)
    private Path sourceFileName;
    private String destinationProtocol;
    private String destinationSystem;
    private Path destinationPath;
    private int maxFiles;

    public String getSourceSHA() {
        return sourceSHA;
    }

    public Path getSourceFileName() {
        return sourceFileName;
    }

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

    public int getMaxFiles() {
        return maxFiles;
    }
}
