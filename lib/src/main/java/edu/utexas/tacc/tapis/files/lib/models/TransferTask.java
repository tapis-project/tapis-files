package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TransferTask {

    private int id;
    private String username;
    private String tenantId;
    private String tag;
    private UUID uuid;
    @Schema(type="string", format = "date-time")
    private Instant created;
    @Schema(type="string", format = "date-time")
    private Instant startTime;
    @Schema(type="string", format = "date-time")
    private Instant endTime;
    private TransferTaskStatus status;
    private List<TransferTaskParent> parentTasks;
    private long estimatedTotalBytes;
    private long totalBytesTransferred;
    private int totalTransfers;
    private int completeTransfers;
    private String errorMessage;

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getEstimatedTotalBytes() {
        return estimatedTotalBytes;
    }

    public void setEstimatedTotalBytes(long estimatedTotalBytes) {
        this.estimatedTotalBytes = estimatedTotalBytes;
    }

    public long getTotalBytesTransferred() {
        return totalBytesTransferred;
    }

    public void setTotalBytesTransferred(long totalBytesTransferred) {
        this.totalBytesTransferred = totalBytesTransferred;
    }

    public int getTotalTransfers() {
        return totalTransfers;
    }

    public void setTotalTransfers(int totalTransfers) {
        this.totalTransfers = totalTransfers;
    }

    public int getCompleteTransfers() {
        return completeTransfers;
    }

    public void setCompleteTransfers(int completeTransfers) {
        this.completeTransfers = completeTransfers;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
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
        if (created != null) this.created = Instant.parse(created);
    }


    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
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

    public TransferTaskStatus getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = TransferTaskStatus.valueOf(status);
    }
    public void setStatus(TransferTaskStatus status) {
        this.status = status;
    }


    public List<TransferTaskParent> getParentTasks() {
        return parentTasks;
    }

    public void setParentTasks(List<TransferTaskParent> parentTasks) {
        this.parentTasks = parentTasks;
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
}
