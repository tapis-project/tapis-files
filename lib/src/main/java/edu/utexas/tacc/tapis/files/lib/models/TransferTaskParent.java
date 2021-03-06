package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

public class TransferTaskParent {

    protected int id;
    protected String tenantId;
    protected String username;
    protected TransferURI sourceURI;
    protected TransferURI destinationURI;
    protected UUID uuid;
    protected long totalBytes;
    protected long bytesTransferred;
    protected int taskId;
    protected TransferTaskStatus status;

    @Schema(type="string", format = "date-time")
    protected Instant created;
    @Schema(type="string", format = "date-time")
    protected Instant startTime;
    @Schema(type="string", format = "date-time")
    protected Instant endTime;
    protected List<TransferTaskChild> children;
    protected String errorMessage;

    public TransferTaskParent(){}

    public TransferTaskParent(String tenantId, String username, String sourceURI, String destinationURI) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceURI = new TransferURI(sourceURI);
        this.destinationURI = new TransferURI(destinationURI);
        this.status = TransferTaskStatus.ACCEPTED;
        this.uuid = UUID.randomUUID();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Unique ID of the task.
     * @return uuid
     **/
    @JsonProperty("uuid")
    @Schema(description = "Unique ID of the task.")
    public UUID getUuid() {
        return uuid;
    }
    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Schema(type="string", format = "date-time")
    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    @JsonProperty("created")
    public void setCreated(String created) {
        this.created = Instant.parse(created);
    }


    @Schema(type="string", format = "date-time")
    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    @JsonProperty("startTime")
    public void setStartTime(String startTime) {
        if (startTime != null) this.startTime = Instant.parse(startTime);
    }

    @Schema(type="string", format = "date-time")
    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    @JsonProperty("endTime")
    public void setEndTime(String endTime) {

        if (endTime != null) this.endTime = Instant.parse(endTime);
    }
    public TransferURI getSourceURI() {
        return sourceURI;
    }

    public void setSourceURI(String sourceURI) {
        this.sourceURI = new TransferURI(sourceURI);
    }
    public void setSourceURI(TransferURI sourceURI) {
        this.sourceURI = sourceURI;
    }

    public TransferURI getDestinationURI() {
        return destinationURI;
    }

    public void setDestinationURI(String destinationURI) {
        this.destinationURI = new TransferURI(destinationURI);
    }
    public void setDestinationURI(TransferURI destinationURI) {
        this.destinationURI = destinationURI;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getId() {
        return id;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

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

    public List<TransferTaskChild> getChildren() {
        return children;
    }

    public void setChildren(List<TransferTaskChild> children) {
        this.children = children;
    }

    /**
     * The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED
     * @return status
     **/
    @JsonProperty("status")
    @Schema(example = "PENDING", description = "The status of the task, such as ACCEPTED, IN_PROGRESS, COMPLETED, CANCELLED")
    public TransferTaskStatus getStatus() {
        return status;
    }

    public void setStatus(TransferTaskStatus status) {
        this.status = status;
    }

    public void setStatus(String status) {
        this.status = TransferTaskStatus.valueOf(status);
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransferTaskParent transferTask = (TransferTaskParent) o;
        return Objects.equals(this.uuid, transferTask.uuid) &&
                Objects.equals(this.created, transferTask.created) &&
                Objects.equals(this.status, transferTask.status);
    }

    @Override
    public int hashCode() {
        return  Objects.hash(uuid, created, status);
    }

    @JsonIgnore
    public boolean isTerminal() {
        Set<TransferTaskStatus> terminalStates = new HashSet<>();
        terminalStates.add(TransferTaskStatus.COMPLETED);
        terminalStates.add(TransferTaskStatus.FAILED);
        terminalStates.add(TransferTaskStatus.CANCELLED);
        terminalStates.add(TransferTaskStatus.PAUSED);
        return terminalStates.contains(this.status);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TransferTaskParent.class.getSimpleName() + "[", "]")
            .add("id=" + id)
            .add("taskId=" + taskId)
            .add("tenantId='" + tenantId + "'")
            .add("username='" + username + "'")
            .add("sourceURI='" + sourceURI + "'")
            .add("destinationURI='" + destinationURI + "'")
            .add("uuid=" + uuid)
            .add("totalBytes=" + totalBytes)
            .add("bytesTransferred=" + bytesTransferred)
            .add("status='" + status + "'")
            .add("created=" + created)
            .add("startTime=" + startTime)
            .add("endTime=" + endTime)
            .toString();
    }

}