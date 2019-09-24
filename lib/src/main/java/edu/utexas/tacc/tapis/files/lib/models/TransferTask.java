package edu.utexas.tacc.tapis.files.lib.models;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public class TransferTask   {
  @JsonProperty("uuid")
  private String uuid = null;

  @JsonProperty("created")
  private String created = null;

  @JsonProperty("status")
  private String status = null;

  public TransferTask uuid(String uuid) {
    this.uuid = uuid;
    return this;
  }

  /**
   * Unique ID of the task.
   * @return uuid
   **/
  @JsonProperty("uuid")
  @Schema(description = "Unique ID of the task.")
  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public TransferTask created(String created) {
    this.created = created;
    return this;
  }

  /**
   * Timestamp in UTC of task creation.
   * @return created
   **/
  @JsonProperty("created")
  @Schema(description = "Timestamp in UTC of task creation.")
  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public TransferTask status(String status) {
    this.status = status;
    return this;
  }

  /**
   * The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED
   * @return status
   **/
  @JsonProperty("status")
  @Schema(example = "PENDING", description = "The status of the task, such as PENDING, IN_PROGRESS, COMPLETED, CANCELLED")
  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
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
    return Objects.hash(uuid, created, status);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class TransferTask {\n");

    sb.append("    uuid: ").append(toIndentedString(uuid)).append("\n");
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