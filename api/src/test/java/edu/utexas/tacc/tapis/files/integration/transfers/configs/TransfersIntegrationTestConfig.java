package edu.utexas.tacc.tapis.files.integration.transfers.configs;

import com.google.gson.JsonObject;

import java.nio.file.Path;
import java.util.List;

public class TransfersIntegrationTestConfig {
    private String baseFilesUrl;
    private String tokenUrl;
    private String username;
    private String password;
    public String getBaseFilesUrl() {
        return baseFilesUrl;
    }

    public String getTokenUrl() {
        return tokenUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
