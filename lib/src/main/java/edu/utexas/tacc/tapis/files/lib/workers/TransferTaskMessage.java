package edu.utexas.tacc.tapis.files.lib.workers;


import javax.validation.constraints.NotNull;

/**
 *
 */
public class TransferTaskMessage {

    private String username;
    private String tenantId;
    private String sourceSystemId;
    private String sourcePath;
    private String destinationSystemId;
    private String destinationPath;

    public TransferTaskMessage(@NotNull  String username, @NotNull String tenantId,
                               @NotNull String sourceSystemId, @NotNull String sourcePath,
                               @NotNull String destinationSystemId, @NotNull String destinationPath) {
        this.username = username;
        this.tenantId = tenantId;
        this.sourceSystemId = sourceSystemId;
        this.sourcePath = sourcePath;
        this.destinationSystemId = destinationSystemId;
        this.destinationPath = destinationPath;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getSourceSystemId() {
        return sourceSystemId;
    }

    public void setSourceSystemId(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getDestinationSystemId() {
        return destinationSystemId;
    }

    public void setDestinationSystemId(String destinationSystemId) {
        this.destinationSystemId = destinationSystemId;
    }

    public String getDestinationPath() {
        return destinationPath;
    }

    public void setDestinationPath(String destinationPath) {
        this.destinationPath = destinationPath;
    }
}
