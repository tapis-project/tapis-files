package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;

public class FilePermission   {

    private String tenantId;
    private String username;
    private String systemId;
    private String path;
    private Permission permission;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Permission getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = permission;
    }

    @Override
    public String toString() {
        return "FilePermission{" +
            "systemId='" + systemId + '\'' +
            ", path='" + path + '\'' +
            ", permission=" + permission +
            '}';
    }
}