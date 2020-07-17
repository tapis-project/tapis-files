package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import org.statefulj.persistence.annotations.State;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class TransferTask implements ITransferTask {

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
    protected int retries;

    // status MUST be a string for the FSM to work, enum is not an option
    @State
    protected String status;

    protected Instant created;


    public TransferTask() {
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED.name();
    }

    public TransferTask(String tenantId, String username, String sourceSystemId, String sourcePath, String destinationSystemId, String destinationPath) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceSystemId = sourceSystemId;
        this.sourcePath = sourcePath;
        this.destinationSystemId = destinationSystemId;
        this.destinationPath = destinationPath;
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED.name();
    }
    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }
    /**
     * Unique ID of the task.
     * @return uuid
     **/
    @Override
    @JsonProperty("uuid")
    @Schema(description = "Unique ID of the task.")
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }
    @Override
    public Long getTotalBytes() {
        return totalBytes;
    }

    @Override
    public void setTotalBytes(Long totalBytes) {
        this.totalBytes = totalBytes;
    }

    @Override
    public Long getBytesTransferred() {
        return bytesTransferred;
    }

    @Override
    public void setBytesTransferred(Long bytesTransferred) {
        this.bytesTransferred = bytesTransferred;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String getSourceSystemId() {
        return sourceSystemId;
    }

    @Override
    public void setSourceSystemId(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    @Override
    public String getSourcePath() {
        return sourcePath;
    }

    @Override
    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    @Override
    public String getDestinationSystemId() {
        return destinationSystemId;
    }

    @Override
    public void setDestinationSystemId(String destinationSystemId) {
        this.destinationSystemId = destinationSystemId;
    }

    @Override
    public String getDestinationPath() {
        return destinationPath;
    }

    @Override
    public void setDestinationPath(String destinationPath) {
        this.destinationPath = destinationPath;
    }

    @Override
    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }


    @Override
    @JsonProperty("created")
    @Schema(description = "Timestamp in UTC of task creation.", format = "date-time")
    public Instant getCreated() {
        return created;
    }

    @Override
    public void setCreated(Instant created) {
        this.created = created;
    }

    @Override
    public void setCreated(String created) {
        this.created = Instant.parse(created);
    }

    @Override
    public void setCreated(Timestamp created) {
        this.created = created.toInstant();
    }

    /**
     * The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED
     * @return status
     **/
    @Override
    @JsonProperty("status")
    @Schema(example = "PENDING", description = "The status of the task, such as ACCEPTED, IN_PROGRESS, COMPLETED, CANCELLED")
    public String getStatus() {
        return status;
    }

    @Override
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