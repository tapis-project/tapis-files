package edu.utexas.tacc.tapis.files.api.models;

import javax.validation.constraints.Pattern;

public class CreateDirectoryRequest {

    //TODO: Other illegal characters?
    @Pattern(regexp = "^(?!.*\\.).+", message=". not allowed in path")
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
