package edu.utexas.tacc.tapis.files.api.models;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public class NativeLinuxOpRequest
{
    private NativeLinuxOperation operation;
    private String newPath;


    @Schema(required = true)
    @NotNull
    public NativeLinuxOperation getOperation() {
        return operation;
    }

    public void setOperation(NativeLinuxOperation operation) {
        this.operation = operation;
    }

    @Schema(required = true, description = "Paths must be absolute, ../.. is not allowed")
    @NotNull
    public String getNewPath() {
        return newPath;
    }

    public void setNewPath(String newPath) {
        this.newPath = newPath;
    }
}
