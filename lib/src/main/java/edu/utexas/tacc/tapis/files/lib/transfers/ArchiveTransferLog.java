package edu.utexas.tacc.tapis.files.lib.transfers;

import com.google.gson.stream.JsonWriter;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferLogEntry;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ArchiveTransferLog implements ObservableArchiveInputStream.Observer {
    Logger log = LoggerFactory.getLogger(ArchiveTransferLog.class);
    private long fileBytesRead;
    private long archiveBytesRead;

    List<ArchiveTransferLogEntry> logEntries = new ArrayList<>();

    @Override
    public void file(String name, long size, Date lastModifiedDate, String digest) {
        logEntries.add(new ArchiveTransferLogEntry(name, size, lastModifiedDate, digest));
        this.fileBytesRead += size;
    }

    public void actualBytesRead(long actualBytesRead) {
        this.archiveBytesRead = actualBytesRead;
    }

    public long getFileBytesRead() {
        return fileBytesRead;
    }

    public long getArchiveBytesRead() {
        return archiveBytesRead;
    }

    public List<ArchiveTransferLogEntry> getLogEntries() {
        return logEntries;
    }
}
