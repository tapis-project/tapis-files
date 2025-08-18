package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.uri.TapisUrl;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public class ArchiveTransfer {
    private int id;
    private UUID uuid;
    private String username;
    private String tenantId;
    private Instant created;
    private ArchiveTransferStatus status;
    private String sourceBaseUrl;
    private String destinationBaseUrl;
    Set<String> relativePaths;
    private Instant startTime;
    private Instant endTime;
    private String errorMessage;
    private String srcSharedCtxGrantor;
    private String destSharedCtxGrantor;
    private UUID assignedTo;
    private long fileBytesRead;
    private long archiveBytesRead;

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

    public ArchiveTransferStatus getStatus() {
        return status;
    }

    public void setStatus(ArchiveTransferStatus status) {
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

    public long getFileBytesRead() {
        return fileBytesRead;
    }

    public void setFileBytesRead(long fileBytesRead) {
        this.fileBytesRead = fileBytesRead;
    }

    public long getArchiveBytesRead() {
        return archiveBytesRead;
    }

    public void setArchiveBytesRead(long archiveBytesRead) {
        this.archiveBytesRead = archiveBytesRead;
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

    public UUID getAssignedTo() {
        return assignedTo;
    }

    public void setAssignedTo(UUID assignedTo) {
        this.assignedTo = assignedTo;
    }
}
