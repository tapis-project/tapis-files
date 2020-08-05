package edu.utexas.tacc.tapis.files.lib.models;

import java.time.Instant;

public class FilesNotification {

    private String tenantId;
    private String recipient;
    private String message;
    private Instant created;

    @Override
    public String toString() {
        return "FilesNotification{" +
            "tenantId='" + tenantId + '\'' +
            ", recipient='" + recipient + '\'' +
            ", message='" + message + '\'' +
            ", created=" + created +
            '}';
    }

    public FilesNotification(){};

    public FilesNotification(String tenantId, String recipient, String message) {
        this.tenantId = tenantId;
        this.message = message;
        this.recipient = recipient;
        this.created = Instant.now();
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }
}
