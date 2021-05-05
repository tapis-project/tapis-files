package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService.NativeLinuxOperation;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotNull;

/*
 * Class representing attributes for incoming native linux operation
 */
public class NativeLinuxOpRequest
{
    private NativeLinuxOperation operation; // operation to perform
    private String argument; // argument for the operation

    @Schema(required = true)
    @NotNull
    public NativeLinuxOperation getOperation() { return operation; }

    public void setOperation(NativeLinuxOperation operation) {
        this.operation = operation;
    }

    @Schema(required = true, description = "Argument for native linux operation")
    @NotNull
    public String getArgument() {
        return argument;
    }

    public void setArgument(String a) {
        argument = a;
    }
}
