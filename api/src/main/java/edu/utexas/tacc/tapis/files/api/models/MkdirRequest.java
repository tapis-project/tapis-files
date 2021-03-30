package edu.utexas.tacc.tapis.files.api.models;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;

public class MkdirRequest {

    private String path;

    @NotBlank
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
