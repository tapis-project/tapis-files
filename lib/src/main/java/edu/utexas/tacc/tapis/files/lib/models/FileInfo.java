package edu.utexas.tacc.tapis.files.lib.models;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.*;

public class FileInfo   {
  @JsonProperty("lastModified")
  private String lastModified = null;

  @JsonProperty("name")
  private String name = null;

  @JsonProperty("path")
  private String path = null;

  @JsonProperty("systemId")
  private String systemId = null;

  @JsonProperty("size")
  private Integer size = null;

  public FileInfo lastModified(String lastModified) {
    this.lastModified = lastModified;
    return this;
  }

  /**
   * Get lastModified
   * @return lastModified
   **/
  @JsonProperty("lastModified")
  @Schema(description = "")
  public String getLastModified() {
    return lastModified;
  }

  public void setLastModified(String lastModified) {
    this.lastModified = lastModified;
  }

  public FileInfo name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
   **/
  @JsonProperty("name")
  @Schema(description = "")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public FileInfo path(String path) {
    this.path = path;
    return this;
  }

  /**
   * Get path
   * @return path
   **/
  @JsonProperty("path")
  @Schema(description = "")
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public FileInfo systemId(String systemId) {
    this.systemId = systemId;
    return this;
  }

  /**
   * Get systemId
   * @return systemId
   **/
  @JsonProperty("systemId")
  @Schema(description = "")
  public String getSystemId() {
    return systemId;
  }

  public void setSystemId(String systemId) {
    this.systemId = systemId;
  }

  public FileInfo size(Integer size) {
    this.size = size;
    return this;
  }

  /**
   * size in kB
   * @return size
   **/
  @JsonProperty("size")
  @Schema(description = "size in kB")
  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileInfo fileInfo = (FileInfo) o;
    return Objects.equals(this.lastModified, fileInfo.lastModified) &&
        Objects.equals(this.name, fileInfo.name) &&
        Objects.equals(this.path, fileInfo.path) &&
        Objects.equals(this.systemId, fileInfo.systemId) &&
        Objects.equals(this.size, fileInfo.size);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastModified, name, path, systemId, size);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class FileInfo {\n");

    sb.append("    lastModified: ").append(toIndentedString(lastModified)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    path: ").append(toIndentedString(path)).append("\n");
    sb.append("    systemId: ").append(toIndentedString(systemId)).append("\n");
    sb.append("    size: ").append(toIndentedString(size)).append("\n");
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