package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService.NativeLinuxOperation;

import javax.validation.constraints.NotNull;

/*
 * Class representing attributes for incoming native linux operation
 */
public class NativeLinuxOpRequest
{
    private NativeLinuxOperation operation; // operation to perform
    private String argument; // argument for the operation

    @NotNull
    public NativeLinuxOperation getOperation() { return operation; }

    public void setOperation(NativeLinuxOperation operation) {
        this.operation = operation;
    }

    @NotNull
    public String getArgument() {
        return argument;
    }

    public void setArgument(String a) {
        argument = a;
    }
}
