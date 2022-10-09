package edu.utexas.tacc.tapis.files.lib.models;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Class representing a file or directory on a Tapis System
 */
public class FileInfo
{
    public static final String FILETYPE_FILE = "file";
    public static final String FILETYPE_DIR = "dir";
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

  /**
   * Constructor to create a FileInfo instance from an S3 object
   * @param s3Object - S3 object
   */
  public FileInfo(S3Object s3Object, String systemId, String rootDir)
  {
    Path tmpPath = Paths.get(s3Object.key());
    name = tmpPath.getFileName().toString();
    lastModified = s3Object.lastModified();
    size = s3Object.size();
    path = PathUtils.getFileInfoPathFromS3Key(s3Object.key(), rootDir);
    url = PathUtils.getTapisUrlFromPath(path, systemId);
    try { mimeType = Files.probeContentType(tmpPath); }
    catch (IOException ex) { mimeType = null; }
    // If it ends with a / then it is considered a "directory" else it is a file
//    if (s3Object.key().endsWith("/")) type = "dir"; else type = "file";
    // S3 objects are always files
    type = FILETYPE_FILE;
  }

    public String getUrl() { return url; }
    public void setUrl(String s) { url = s; }

    public String getMimeType() { return mimeType;  }
    public void setMimeType(String mt) { mimeType = mt; }

    public String getType() { return type; }
    public void setType(String s) { type = s; }

    public String getOwner() { return owner; }
    public void setOwner(String s) { owner = s; }

    public String getGroup() { return group; }
    public void setGroup(String s) { group = s; }

    public String getNativePermissions() { return nativePermissions; }
    public void setNativePermissions(String s) { nativePermissions = s; }

    @JsonIgnore
    public boolean isDir() { return type.equals(FILETYPE_DIR); }

    @JsonProperty("lastModified")
    public Instant getLastModified() { return lastModified; }
    public void setLastModified(Instant i) { lastModified = i; }
    public void setLastModified(String s) { lastModified = Instant.parse(s); }

    @JsonProperty("name")
    public String getName() { return name; }
    public void setName(String s) { name = s; }

    @JsonProperty("path")
    public String getPath() { return path; }
    public void setPath(String s) { path = s; }

    // Size in kB
    @JsonProperty("size")
    public long getSize() { return size; }
    public void setSize(Long l) { size = l; }
    public void setSize(long l) { size = l; }

    @Override
    public boolean equals(java.lang.Object o)
    {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      FileInfo fileInfo = (FileInfo) o;
      return Objects.equals(lastModified, fileInfo.lastModified) &&
              Objects.equals(name, fileInfo.name) &&
              Objects.equals(path, fileInfo.path) &&
              Objects.equals(size, fileInfo.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastModified, name, path, size);
    }

    @Override
    public String toString()
    {
      return "FileInfo {\n" +
             "    lastModified: " + toIndentedString(lastModified) + "\n" +
             "    name: " + toIndentedString(name) + "\n" +
             "    path: " + toIndentedString(path) + "\n" +
             "    url:  " + toIndentedString(url) + "\n" +
             "    size: " + toIndentedString(size) + "\n" +
             "    type: " + toIndentedString(type) + "\n" +
             "}";
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o)
    {
      if (o == null) return "null";
      return o.toString().replace("\n", "\n    ");
    }
}