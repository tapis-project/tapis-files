package edu.utexas.tacc.tapis.files.lib.transfers;

import java.util.ArrayList;
import java.util.List;

public class ArchiveTransferLog implements ObservableArchiveInputStream.Observer {
    private long fileBytesRead;
    private long archiveBytesRead;
    private List<String> transferInfo = new ArrayList<>();

    @Override
    public void file(String name, long bytesRead, String digest) {
        StringBuilder fileInfo = new StringBuilder();
        fileInfo.append("Name : ");
        fileInfo.append(name);
        fileInfo.append(" Digest : ");
        fileInfo.append(digest);
        fileInfo.append(" Bytes : ");
        fileInfo.append(bytesRead);
        fileInfo.append(System.lineSeparator());
        transferInfo.add(fileInfo.toString());
        this.fileBytesRead += bytesRead;
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

    public List<String> getTransferInfo() {
        return transferInfo;
    }
}
