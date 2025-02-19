package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import java.nio.file.Path;

public class CleanupConfig {
    private String system;
    private Path path;
    private String pattern;

    public String getSystem() {
        return system;
    }

    public Path getPath() {
        return path;
    }

    public String getPattern() {
        return pattern;
    }
}
