package edu.utexas.tacc.tapis.files.lib.clients;

import com.fasterxml.jackson.annotation.JsonFormat;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;

public class RemoteFileInfo {

    @JsonFormat(shape = JsonFormat.Shape.STRING, timezone = "UTC")
    private Instant lastModified;

    private String name;
    private Long size;

    public RemoteFileInfo(S3Object listing) {
        this.name = listing.key();
        this.lastModified = listing.lastModified();
        this.size = listing.size();

    }

    public Instant getLastModified() {
        return lastModified;
    }

    public void setLastModified(Instant lastModified) {
        this.lastModified = lastModified;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }
}
