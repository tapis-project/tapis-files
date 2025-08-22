package edu.utexas.tacc.tapis.files.lib.services;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class ArchiveTransferResponse {
    private int id;
    private UUID uuid;
    private String username;
    private String tenantId;
    private Instant created;
    private String status;
    private String sourceBaseUrl;
    private String destinationBaseUrl;
    Set<String> relativePaths;
    private Instant startTime;
    private Instant endTime;
    private Long archiveBytesRead;
    private Long fileBytesRead;
    private String errorMessage;
    private String srcSharedCtxGrantor;
    private String destSharedCtxGrantor;
    private String archiveType;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
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

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    public String getSourceBaseUrl() {
        return sourceBaseUrl;
    }

    public void setSourceBaseUrl(String sourceBaseUrl) {
        this.sourceBaseUrl = sourceBaseUrl;
    }
    public String getDestinationBaseUrl() {
        return destinationBaseUrl;
    }

    public void setDestinationBaseUrl(String destinationBaseUrl) {
        this.destinationBaseUrl = destinationBaseUrl;
    }

    public Set<String> getRelativePaths() {
        return relativePaths;
    }

    public void setRelativePaths(Set<String> relativePaths) {
        this.relativePaths = relativePaths;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    public Long getArchiveBytesRead() {
        return archiveBytesRead;
    }

    public void setArchiveBytesRead(Long archiveBytesRead) {
        this.archiveBytesRead = archiveBytesRead;
    }

    public Long getFileBytesRead() {
        return fileBytesRead;
    }

    public void setFileBytesRead(Long fileBytesRead) {
        this.fileBytesRead = fileBytesRead;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getSrcSharedCtxGrantor() {
        return srcSharedCtxGrantor;
    }

    public void setSrcSharedCtxGrantor(String srcSharedCtxGrantor) {
        this.srcSharedCtxGrantor = srcSharedCtxGrantor;
    }

    public String getDestSharedCtxGrantor() {
        return destSharedCtxGrantor;
    }

    public void setDestSharedCtxGrantor(String destSharedCtxGrantor) {
        this.destSharedCtxGrantor = destSharedCtxGrantor;
    }

    public String getArchiveType() {
        return archiveType;
    }

    public void setArchiveType(String archiveType) {
        this.archiveType = archiveType;
    }
}
