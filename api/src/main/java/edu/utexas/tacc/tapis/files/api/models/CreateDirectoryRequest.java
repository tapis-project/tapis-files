package edu.utexas.tacc.tapis.files.api.models;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class CreateDirectoryRequest {

    //TODO: Other illegal characters?
    @Pattern(regexp = "^(?!.*\\.).+", message=". not allowed in path")
    @Schema(required = true, description = "Path of folder")
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
