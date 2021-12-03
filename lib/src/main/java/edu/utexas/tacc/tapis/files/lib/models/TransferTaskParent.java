package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

public class TransferTaskParent
{
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
  protected boolean optional;

  protected Instant created;
  protected Instant startTime;
  protected Instant endTime;
  protected List<TransferTaskChild> children;
  protected String errorMessage;

  public TransferTaskParent(){}

  public TransferTaskParent(String tenantId1, String username1, String srcURI1, String dstURI1, boolean optional1)
  {
    tenantId = tenantId1;
    username = username1;
    sourceURI = new TransferURI(srcURI1);
    destinationURI = new TransferURI(dstURI1);
    status = TransferTaskStatus.ACCEPTED;
    optional = optional1;
    uuid = UUID.randomUUID();
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
  public UUID getUuid() {
    return uuid;
  }
  public void setUuid(UUID uuid) {
    this.uuid = uuid;
  }

  public void setId(int id) {
    this.id = id;
  }

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

  public Instant getEndTime() {
    return endTime;
  }

  public void setEndTime(Instant endTime) {
    this.endTime = endTime;
  }

  @JsonProperty("endTime")
  public void setEndTime(String s) { if (s != null) endTime = Instant.parse(s); }
  public TransferURI getSourceURI() {
    return sourceURI;
  }

  public void setSourceURI(String s) {
    sourceURI = new TransferURI(s);
  }
  public void setSourceURI(TransferURI t) {
    sourceURI = t;
  }

  public TransferURI getDestinationURI() {
    return destinationURI;
  }
  public void setDestinationURI(String s) {
    destinationURI = new TransferURI(s);
  }
  public void setDestinationURI(TransferURI t) {
    destinationURI = t;
  }

  public int getTaskId() {
    return taskId;
  }
  public void setTaskId(int i) {
    taskId = i;
  }

  public int getId() {
    return id;
  }

  public long getTotalBytes() {
    return totalBytes;
  }
  public void setTotalBytes(long l) {
    totalBytes = l;
  }

  public long getBytesTransferred() {
    return bytesTransferred;
  }
  public void setBytesTransferred(long l) {
    bytesTransferred = l;
  }

  public String getTenantId() {
    return tenantId;
  }
  public void setTenantId(String s) { tenantId = s; }

  public String getUsername() {
    return username;
  }
  public void setUsername(String s) { username = s; }

  public boolean isOptional() { return optional; }
  public void setOptional(boolean b) { optional = b; }

  public List<TransferTaskChild> getChildren() {
    return children;
  }
  public void setChildren(List<TransferTaskChild> tlist) {
    children = tlist;
  }

  /**
   * The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED
   * @return status
   **/
  @JsonProperty("status")
  public TransferTaskStatus getStatus() {
    return status;
  }
  public void setStatus(TransferTaskStatus tts) {
    status = tts;
  }
  public void setStatus(String s) {
    status = TransferTaskStatus.valueOf(s);
  }

  // Support for equals
  @Override
  public boolean equals(java.lang.Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
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
  public boolean isTerminal()
  {
    Set<TransferTaskStatus> terminalStates = new HashSet<>();
    terminalStates.add(TransferTaskStatus.COMPLETED);
    terminalStates.add(TransferTaskStatus.FAILED);
    terminalStates.add(TransferTaskStatus.FAILED_OPT);
    terminalStates.add(TransferTaskStatus.CANCELLED);
    terminalStates.add(TransferTaskStatus.PAUSED);
    return terminalStates.contains(this.status);
  }

  @Override
  public String toString()
  {
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
            .add("optional=" + optional)
            .add("created=" + created)
            .add("startTime=" + startTime)
            .add("endTime=" + endTime)
            .toString();
  }
}