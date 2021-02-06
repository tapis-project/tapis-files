package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.models.FilePermissionsEnum;

import javax.validation.constraints.NotNull;

public class CreatePermissionRequest {

    @NotNull
    private String username;

    @NotNull
    private FilePermissionsEnum permission;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public FilePermissionsEnum getPermission() {
        return permission;
    }

    public void setPermission(FilePermissionsEnum permission) {
        this.permission = permission;
    }
}
