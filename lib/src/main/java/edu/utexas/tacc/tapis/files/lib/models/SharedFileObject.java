package edu.utexas.tacc.tapis.files.lib.models;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SharedFileObject   {
  @JsonProperty("creator")
  private String creator = null;

  @JsonProperty("sharedWith")
  private String sharedWith = null;

  @JsonProperty("created")
  private String created = null;

  @JsonProperty("expiresIn")
  private Integer expiresIn = null;

  @JsonProperty("url")
  private String url = null;

  public SharedFileObject creator(String creator) {
    this.creator = creator;
    return this;
  }

  /**
   * Username who shared the file/folder
   * @return creator
   **/
  @JsonProperty("creator")
  public String getCreator() {
    return creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public SharedFileObject sharedWith(String sharedWith) {
    this.sharedWith = sharedWith;
    return this;
  }

  /**
   * Username who was granted access
   * @return sharedWith
   **/
  @JsonProperty("sharedWith")
  public String getSharedWith() {
    return sharedWith;
  }

  public void setSharedWith(String sharedWith) {
    this.sharedWith = sharedWith;
  }

  public SharedFileObject created(String created) {
    this.created = created;
    return this;
  }

  /**
   * Creation timestamp in UTC
   * @return created
   **/
  @JsonProperty("created")
  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public SharedFileObject expiresIn(Integer expiresIn) {
    this.expiresIn = expiresIn;
    return this;
  }

  /**
   * Number of seconds in which the share was set to expire.
   * @return expiresIn
   **/
  @JsonProperty("expiresIn")
  public Integer getExpiresIn() {
    return expiresIn;
  }

  public void setExpiresIn(Integer expiresIn) {
    this.expiresIn = expiresIn;
  }

  public SharedFileObject url(String url) {
    this.url = url;
    return this;
  }

  /**
   * Link to the shared file.
   * @return url
   **/
  @JsonProperty("url")
  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SharedFileObject sharedFileObject = (SharedFileObject) o;
    return Objects.equals(this.creator, sharedFileObject.creator) &&
        Objects.equals(this.sharedWith, sharedFileObject.sharedWith) &&
        Objects.equals(this.created, sharedFileObject.created) &&
        Objects.equals(this.expiresIn, sharedFileObject.expiresIn) &&
        Objects.equals(this.url, sharedFileObject.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(creator, sharedWith, created, expiresIn, url);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SharedFileObject {\n");

    sb.append("    creator: ").append(toIndentedString(creator)).append("\n");
    sb.append("    sharedWith: ").append(toIndentedString(sharedWith)).append("\n");
    sb.append("    created: ").append(toIndentedString(created)).append("\n");
    sb.append("    expiresIn: ").append(toIndentedString(expiresIn)).append("\n");
    sb.append("    url: ").append(toIndentedString(url)).append("\n");
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