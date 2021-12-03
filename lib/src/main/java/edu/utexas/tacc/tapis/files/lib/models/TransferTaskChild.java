package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.StringJoiner;
import java.util.UUID;

public class TransferTaskChild extends TransferTaskParent
{
    private int parentTaskId;
    private int retries;
    private boolean isDir;

    public TransferTaskChild() {}

    public TransferTaskChild(String tenantId, String username,
                             TransferURI sourceURI, TransferURI destinationURI,
                             int parentTaskId) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceURI = sourceURI;
        this.destinationURI = destinationURI;
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED;
        this.parentTaskId = parentTaskId;
    }

    /**
     *  @param transferTaskParent The TransferTask from which to derive this child
     * @param fileInfo The path to the single file in the source system
     */
    public TransferTaskChild(@NotNull TransferTaskParent transferTaskParent, @NotNull FileInfo fileInfo) throws TapisException {

        TransferURI sourceURL = transferTaskParent.getSourceURI();
        TransferURI destURL =transferTaskParent.getDestinationURI();

        Path destPath = PathUtils.relativizePaths(
            sourceURL.getPath(),
            fileInfo.getPath(),
            destURL.getPath()
        );

        TransferURI newSourceURL = new TransferURI(sourceURL.getProtocol(), sourceURL.getSystemId(), fileInfo.getPath());
        TransferURI newDestURL = new TransferURI(destURL.getProtocol(), destURL.getSystemId(), destPath.toString());

        this.setParentTaskId(transferTaskParent.getId());
        this.setTaskId(transferTaskParent.getTaskId());
        this.setSourceURI(newSourceURL.toString());
        this.setDestinationURI(newDestURL.toString());
        this.setStatus(TransferTaskStatus.ACCEPTED.name());
        this.setTenantId(transferTaskParent.getTenantId());
        this.setUsername(transferTaskParent.getUsername());
        this.setBytesTransferred(0L);
        this.setTotalBytes(fileInfo.getSize());
        this.setDir(fileInfo.isDir());
    }

    public int getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(int parentTaskId) { this.parentTaskId = parentTaskId; }
    public int getRetries() { return retries; }
    public void setRetries(int retries) { this.retries = retries; }
    public boolean isDir() {
        return isDir;
    }
    public void setDir(boolean dir) {
        this.isDir = dir;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TransferTaskChild that = (TransferTaskChild) o;

        return parentTaskId == that.parentTaskId;
    }

    @Override
    public int hashCode() { return Long.hashCode(id); }

    @Override
    public String toString() {
        return new StringJoiner(", ", TransferTaskChild.class.getSimpleName() + "[", "]")
            .add("id=" + id)
            .add("parentTaskId=" + parentTaskId)
            .add("taskId=" + getTaskId())
            .add("retries=" + retries)
            .add("tenantId='" + tenantId + "'")
            .add("username='" + username + "'")
            .add("sourceURI='" + sourceURI + "'")
            .add("destinationURI='" + destinationURI + "'")
            .add("uuid=" + uuid)
            .add("totalBytes=" + totalBytes)
            .add("bytesTransferred=" + bytesTransferred)
            .add("status='" + status + "'")
            .add("created=" + created)
            .add("startTime=" + startTime)
            .add("endTime=" + endTime)
            .toString();
    }
}
