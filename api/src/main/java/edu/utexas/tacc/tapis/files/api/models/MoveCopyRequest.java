package edu.utexas.tacc.tapis.files.api.models;

import javax.validation.constraints.NotNull;

public class MoveCopyRequest {

    private MoveCopyOperation operation;
    private String newPath;

    @NotNull
    public MoveCopyOperation getOperation() {
        return operation;
    }

    public void setOperation(MoveCopyOperation operation) {
        this.operation = operation;
    }

    @NotNull
    public String getNewPath() {
        return newPath;
    }

    public void setNewPath(String newPath) {
        this.newPath = newPath;
    }
}
