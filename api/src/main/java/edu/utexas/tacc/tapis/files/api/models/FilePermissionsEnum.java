package edu.utexas.tacc.tapis.files.api.models;

public enum FilePermissionsEnum {
    READ("r"),
    READWRITE("rw"),
    ALL("*");

    private final String label;

    private FilePermissionsEnum(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

}
