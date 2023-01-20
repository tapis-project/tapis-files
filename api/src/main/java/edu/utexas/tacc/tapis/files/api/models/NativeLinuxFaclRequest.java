package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;

public class NativeLinuxFaclRequest {
    FileUtilsService.NativeLinuxFaclOperation operation;
    FileUtilsService.NativeLinuxFaclRecursion recursionMethod;
    String aclString;

    public NativeLinuxFaclRequest() {
        recursionMethod = FileUtilsService.NativeLinuxFaclRecursion.NONE;
    }
    public FileUtilsService.NativeLinuxFaclOperation getOperation() {
        return operation;
    }

    public void setOperation(FileUtilsService.NativeLinuxFaclOperation operation) {
        this.operation = operation;
    }

    public FileUtilsService.NativeLinuxFaclRecursion getRecursionMethod() {
        return recursionMethod;
    }

    public void setRecursionMethod(FileUtilsService.NativeLinuxFaclRecursion recursionMethod) {
        this.recursionMethod = recursionMethod;
    }

    public String getAclString() {
        return aclString;
    }

    public void setAclString(String aclString) {
        this.aclString = aclString;
    }
}
