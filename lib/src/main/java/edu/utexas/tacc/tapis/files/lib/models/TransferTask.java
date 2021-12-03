package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TransferTask
{
    protected int id;
    protected String username;
    protected String tenantId;
    protected String tag;
    protected UUID uuid;
    protected Instant created;
    protected Instant startTime;
    protected Instant endTime;
    protected TransferTaskStatus status;
    protected List<TransferTaskParent> parentTasks;
    protected long estimatedTotalBytes;
    protected long totalBytesTransferred;
    protected int totalTransfers;
    protected int completeTransfers;
    protected String errorMessage;

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String s) { errorMessage = s; }

    public long getEstimatedTotalBytes() { return estimatedTotalBytes; }
    public void setEstimatedTotalBytes(long l) { estimatedTotalBytes = l; }

    public long getTotalBytesTransferred() { return totalBytesTransferred; }
    public void setTotalBytesTransferred(long l) { totalBytesTransferred = l; }

    public int getTotalTransfers() { return totalTransfers; }
    public void setTotalTransfers(int i) { totalTransfers = i; }

    public int getCompleteTransfers() { return completeTransfers; }
    public void setCompleteTransfers(int i) { completeTransfers = i; }

    public int getId() { return id; }
    public void setId(int i) { id = i; }

    public String getUsername() { return username; }
    public void setUsername(String s) { username = s; }

    public String getTenantId() { return tenantId; }
    public void setTenantId(String s) { tenantId = s; }

    public String getTag() { return tag; }
    public void setTag(String s) { tag = s; }

    public Instant getCreated() { return created;}
    public void setCreated(Instant c1) { created = c1; }
    @JsonProperty("created")
    public void setCreated(String c1) { if (c1 != null) created = Instant.parse(c1); }

    public UUID getUuid() { return uuid; }
    public void setUuid(UUID u) { uuid = u; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant start) { startTime = start; }
    @JsonProperty("startTime")
    public void setStartTime(String s) { if (s != null) startTime = Instant.parse(s); }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant et) { endTime = et; }
    @JsonProperty("endTime")
    public void setEndTime(String et) { if (et != null) endTime = Instant.parse(et); }

    public TransferTaskStatus getStatus() { return status; }
    public void setStatus(String s) { status = TransferTaskStatus.valueOf(s); }
    public void setStatus(TransferTaskStatus tts) { status = tts; }

    public List<TransferTaskParent> getParentTasks() { return parentTasks; }
    public void setParentTasks(List<TransferTaskParent> pt) { parentTasks = pt; }

    @JsonIgnore
    public boolean isTerminal()
    {
        Set<TransferTaskStatus> terminalStates = new HashSet<>();
        terminalStates.add(TransferTaskStatus.COMPLETED);
        terminalStates.add(TransferTaskStatus.FAILED);
        terminalStates.add(TransferTaskStatus.CANCELLED);
        terminalStates.add(TransferTaskStatus.PAUSED);
        return terminalStates.contains(status);
    }
}
