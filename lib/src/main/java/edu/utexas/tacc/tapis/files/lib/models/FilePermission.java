package edu.utexas.tacc.tapis.files.lib.models;

public class FilePermission   {

    private String tenantId;
    private String username;
    private String systemId;
    private String path;
    private FilePermissionsEnum permissions;

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

    public FilePermissionsEnum getPermissions() {
        return permissions;
    }

    public void setPermissions(FilePermissionsEnum permissions) {
        this.permissions = permissions;
    }

    @Override
    public String toString() {
        return "FilePermission{" +
            "systemId='" + systemId + '\'' +
            ", path='" + path + '\'' +
            ", permissions=" + permissions +
            '}';
    }
}