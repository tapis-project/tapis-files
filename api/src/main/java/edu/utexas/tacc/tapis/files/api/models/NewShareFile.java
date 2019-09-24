package edu.utexas.tacc.tapis.files.api.models;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.*;

public class NewShareFile   {
  @JsonProperty("username")
  private String username = null;

  @JsonProperty("expiresIn")
  private Integer expiresIn = null;

  public NewShareFile username(String username) {
    this.username = username;
    return this;
  }

  /**
   * The user with which to share
   * @return username
   **/
  @JsonProperty("username")
  @Schema(required = true, description = "The user with which to share")
  @NotNull
  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public NewShareFile expiresIn(Integer expiresIn) {
    this.expiresIn = expiresIn;
    return this;
  }

  /**
   * Time in seconds of expiration. minimum&#x3D;1, maximum&#x3D;604800 (1 week)
   * minimum: 1
   * maximum: 604800
   * @return expiresIn
   **/
  @JsonProperty("expiresIn")
  @Schema(required = true, description = "Time in seconds of expiration. minimum=1, maximum=604800 (1 week)")
  @NotNull
  @Min(1) @Max(604800)  public Integer getExpiresIn() {
    return expiresIn;
  }

  public void setExpiresIn(Integer expiresIn) {
    this.expiresIn = expiresIn;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NewShareFile newShareFile = (NewShareFile) o;
    return Objects.equals(this.username, newShareFile.username) &&
        Objects.equals(this.expiresIn, newShareFile.expiresIn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, expiresIn);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class NewShareFile {\n");

    sb.append("    username: ").append(toIndentedString(username)).append("\n");
    sb.append("    expiresIn: ").append(toIndentedString(expiresIn)).append("\n");
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