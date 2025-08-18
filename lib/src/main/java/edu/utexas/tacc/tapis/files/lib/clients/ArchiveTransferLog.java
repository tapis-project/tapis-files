package edu.utexas.tacc.tapis.files.lib.clients;

import java.util.ArrayList;
import java.util.List;

public class ArchiveTransferLog implements ObservableTapisArchiveInputStream.Observer {
    private long fileBytesRead;
    private long archiveBytesRead;
    private List<String> transferInfo = new ArrayList<>();

    @Override
    public void file(String path, String name, long bytesRead, String digest) {
        StringBuilder fileInfo = new StringBuilder();
        fileInfo.append("Path : ");
        fileInfo.append(path);
        fileInfo.append(" Name : ");
        fileInfo.append(name);
        fileInfo.append(" Digest : ");
        fileInfo.append(digest);
        fileInfo.append(" Bytes : ");
        fileInfo.append(bytesRead);
        fileInfo.append(System.lineSeparator());
        transferInfo.add(fileInfo.toString());
    }

    public void total(long archiveBytesRead, long fileBytesRead) {
        this.archiveBytesRead = archiveBytesRead;
        this.fileBytesRead = fileBytesRead;
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
