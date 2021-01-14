package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class TransferTaskParent {

    protected int id;
    protected String tenantId;
    protected String username;
    protected String sourceURI;
    protected String destinationURI;
    protected UUID uuid;
    protected long totalBytes;
    protected long bytesTransferred;
    private int taskId;
    protected String status;

    protected Instant created;
    protected Instant startTime;
    protected Instant endTime;



    public TransferTaskParent(){};

    public TransferTaskParent(String tenantId, String username, String sourceURI, String destinationURI) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceURI = sourceURI;
        this.destinationURI = destinationURI;
        this.status = TransferTaskStatus.ACCEPTED.name();
        this.uuid = UUID.randomUUID();
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

    public String getSourceURI() {
        return sourceURI;
    }

    public void setSourceURI(String sourceURI) {
        this.sourceURI = sourceURI;
    }

    public String getDestinationURI() {
        return destinationURI;
    }

    public void setDestinationURI(String destinationURI) {
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



    @JsonProperty("created")
    @Schema(description = "Timestamp in UTC of task creation.", format = "date-time")
    public Instant getCreated() {
        return created;
    }

    @JsonIgnore
    public void setCreated(Instant created) {
        this.created = created;
    }

    public void setCreated(String created) {
        this.created = Instant.parse(created);
    }

    public void setCreated(Timestamp created) {
        this.created = created.toInstant();
    }

    /**
     * The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED
     * @return status
     **/
    @JsonProperty("status")
    @Schema(example = "PENDING", description = "The status of the task, such as ACCEPTED, IN_PROGRESS, COMPLETED, CANCELLED")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) throws IllegalArgumentException {
        this.status = TransferTaskStatus.valueOf(status).name();
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


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class TransferTask {\n");
        sb.append("    id: ").append(toIndentedString(id)).append("\n");
        sb.append("    uuid: ").append(toIndentedString(uuid)).append("\n");
        sb.append("    tenantId: ").append(toIndentedString(tenantId)).append("\n");
        sb.append("    created: ").append(toIndentedString(created)).append("\n");
        sb.append("    status: ").append(toIndentedString(status)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}