package edu.utexas.tacc.tapis.files.lib.models;

import java.util.Date;

public class ArchiveTransferLogEntry {
    private final String name;
    private final String digest;
    private final Date lastModifiedDate;
    private final long size;

    public ArchiveTransferLogEntry(String name, long size, Date lastModifiedDate, String digest) {
       this.name = name;
       this.lastModifiedDate = lastModifiedDate;
       this.size = size;
       this.digest = digest;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public Date getLastModifiedDate() {
        return lastModifiedDate;
    }

    public String getDigest() {
        return digest;
    }
}
