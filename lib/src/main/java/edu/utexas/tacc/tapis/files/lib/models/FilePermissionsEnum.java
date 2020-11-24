package edu.utexas.tacc.tapis.files.lib.models;

public enum FilePermissionsEnum {
    READ("r"),
    READWRITE("rw"),
    ALL("*");

    private final String label;

    FilePermissionsEnum(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

}
