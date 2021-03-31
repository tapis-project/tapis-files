package edu.utexas.tacc.tapis.files.api.models;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;

import javax.validation.constraints.NotNull;

public class CreatePermissionRequest {

    @NotNull
    private String username;

    @NotNull
    private Permission permission;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Permission getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = permission;
    }
}
