package edu.utexas.tacc.tapis.files.api.models;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

public class MkdirRequest {

    private String path;

    @NotBlank
    @Pattern(regexp = "^(?!.*\\.).+", message = ". not allowed in path")
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
