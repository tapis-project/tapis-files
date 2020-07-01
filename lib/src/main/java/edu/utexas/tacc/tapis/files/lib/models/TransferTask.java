package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.statefulj.persistence.annotations.State;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class TransferTask   {

    protected int id;
    protected String tenantId;
    protected String username;
    protected String sourceSystemId;
    protected String sourcePath;
    protected String destinationSystemId;
    protected String destinationPath;
    protected UUID uuid;
    protected Long totalBytes;
    protected Long bytesTransferred;

    @State
    protected TransferTaskStatus status;

    protected Instant created;


    public TransferTask() {
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED;
    }

    public TransferTask(String tenantId, String username, String sourceSystemId, String sourcePath, String destinationSystemId, String destinationPath) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceSystemId = sourceSystemId;
        this.sourcePath = sourcePath;
        this.destinationSystemId = destinationSystemId;
        this.destinationPath = destinationPath;
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED;
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

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
    public Long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(Long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public Long getBytesTransferred() {
        return bytesTransferred;
    }

    public void setBytesTransferred(Long bytesTransferred) {
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

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }


    @JsonProperty("created")
    @Schema(description = "Timestamp in UTC of task creation.", format = "date-time")
    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public void setCreated(String created) {
        this.created = Instant.parse(created);
    }

    /**
     * The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED
     * @return status
     **/
    @JsonProperty("status")
    @Schema(example = "PENDING", description = "The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED")
    public TransferTaskStatus getStatus() {
        return status;
    }

    public void setStatus(TransferTaskStatus status) {
        this.status = status;
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransferTask transferTask = (TransferTask) o;
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