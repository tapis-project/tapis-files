package edu.utexas.tacc.tapis.files.lib.models;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.model.S3Object;

public class FileInfo   {

    public enum Permission {READ, MODIFY}

    @JsonProperty("lastModified")
    private Instant lastModified = null;

    @JsonProperty("name")
    private String name = null;

    @JsonProperty("path")
    private String path = null;

    @JsonProperty("size")
    private Long size = null;

    private String mimeType;
    private String type;
    private String owner;
    private String group;
    private String nativePermissions;
    private String url;

  public FileInfo() {}

  public FileInfo(S3Object listing) {
    Path tmpPath = Paths.get(listing.key());
    this.name = tmpPath.getFileName().toString();
    this.lastModified = listing.lastModified();
    this.size = listing.size();
    this.path = StringUtils.removeStart(listing.key(), "/");
    try {
      this.mimeType = Files.probeContentType(tmpPath);
    } catch (IOException ex) {
      this.mimeType = null;
    }
    if (listing.key().endsWith("/")) {
      this.type = "dir";
    } else {
      this.type = "file";
    }
  }

    public String getUrl() {
        return url;
    }
    public void setUrl(String s) { url = s; }

    public String getMimeType() {
        return mimeType;
    }
    public void setMimeType(String mt) { mimeType = mt; }

    public String getType() {
        return type;
    }
    public void setType(String s) {
        type = s;
    }

    public String getOwner() {
        return owner;
    }
    public void setOwner(String s) {
        owner = s;
    }

    public String getGroup() {
        return group;
    }
    public void setGroup(String s) {
        group = s;
    }

    public String getNativePermissions() {
        return nativePermissions;
    }
    public void setNativePermissions(String s) { nativePermissions = s; }

    @JsonIgnore
    public boolean isDir() {
        return this.type.equals("dir");
    }

    /**
     * Get lastModified
     * @return lastModified
     **/
    @JsonProperty("lastModified")
    public Instant getLastModified() {
        return lastModified;
    }
    public void setLastModified(Instant i) {
        lastModified = i;
    }
    public void setLastModified(String s) {
        lastModified = Instant.parse(s);
    }

    /**
     * Get name
     * @return name
     **/
    @JsonProperty("name")
    public String getName() {
        return name;
    }
    public void setName(String s) {
        name = s;
    }


    /**
     * Get path
     * @return path
     **/
    @JsonProperty("path")
    public String getPath() { return path; }
    public void setPath(String s) { path = s; }

    /**
     * size in kB
     * @return size
     **/
    @JsonProperty("size")
    public long getSize() {
        return size;
    }
    public void setSize(Long l) {
    size = l;
  }
    public void setSize(long l) {
        size = l;
    }


    @Override
    public boolean equals(java.lang.Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        FileInfo fileInfo = (FileInfo) o;
        return Objects.equals(this.lastModified, fileInfo.lastModified) &&
                Objects.equals(this.name, fileInfo.name) &&
                Objects.equals(this.path, fileInfo.path) &&
                Objects.equals(this.size, fileInfo.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastModified, name, path, size);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class FileInfo {\n");

        sb.append("    lastModified: ").append(toIndentedString(lastModified)).append("\n");
        sb.append("    name: ").append(toIndentedString(name)).append("\n");
        sb.append("    path: ").append(toIndentedString(path)).append("\n");
        sb.append("    size: ").append(toIndentedString(size)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) return "null";
        return o.toString().replace("\n", "\n    ");
    }
}