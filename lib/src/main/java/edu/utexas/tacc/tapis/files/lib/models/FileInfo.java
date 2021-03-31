package edu.utexas.tacc.tapis.files.lib.models;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
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
    private String permission;
    private String uri;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public FileInfo(S3Object listing) {
        Path tmpPath = Paths.get(listing.key());
        this.name = tmpPath.getFileName().toString();
        this.lastModified = listing.lastModified();
        this.size = listing.size();
        this.path = listing.key();
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
    public FileInfo() {}
    public void setSize(Long size) {
        this.size = size;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPermission() {
        return permission;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    @JsonIgnore
    public boolean isDir() {
        return this.path.endsWith("/");
    }


    /**
     * Get lastModified
     * @return lastModified
     **/
    @JsonProperty("lastModified")
    @Schema(type="string", format = "date-time")
    public Instant getLastModified() {
        return lastModified;
    }

    public void setLastModified(Instant lastModified) {
        this.lastModified = lastModified;
    }
    public void setLastModified(String lastModified) {
        this.lastModified = Instant.parse(lastModified);
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


    /**
     * size in kB
     * @return size
     **/
    @JsonProperty("size")
    @Schema(description = "size in kB")
    public long getSize() {
        return size;
    }

    public void setSize(long size) {
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
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}