package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.UUID;

public class TransferTaskChild extends TransferTask implements ITransferTask {

    private long parentTaskId;

    public TransferTaskChild() {}

    public TransferTaskChild(String tenantId, String username,
                             String sourceSystemId, String sourcePath,
                             String destinationSystemId, String destinationPath, long parentTaskId) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceSystemId = sourceSystemId;
        this.sourcePath = sourcePath;
        this.destinationSystemId = destinationSystemId;
        this.destinationPath = destinationPath;
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED.name();
        this.parentTaskId = parentTaskId;
    }

    /**
     *  @param transferTask The TransferTask from which to derive this child
     * @param fileInfo The path to the single file in the source system
     */
    public TransferTaskChild(@NotNull TransferTask transferTask, @NotNull FileInfo fileInfo) {

        Path destPath = PathUtils.relativizePathsForTransfer(
            transferTask.getSourcePath(),
            fileInfo.getPath(),
            transferTask.getDestinationPath()
        );

        this.setParentTaskId(transferTask.getId());
        this.setSourcePath(fileInfo.getPath());
        this.setSourceSystemId(transferTask.getSourceSystemId());
        this.setParentTaskId(transferTask.getId());
        this.setDestinationPath(destPath.toString());
        this.setDestinationSystemId(transferTask.getDestinationSystemId());
        this.setStatus(TransferTaskStatus.ACCEPTED.name());
        this.setTenantId(transferTask.getTenantId());
        this.setUsername(transferTask.getUsername());
        this.setBytesTransferred(0L);
        this.setTotalBytes(fileInfo.getSize());
    }

    public long getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(long parentTaskId) {
        this.parentTaskId = parentTaskId;
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
    public int hashCode() {
       return Long.hashCode(id);
    }

    @Override
    public String toString() {
        return "TransferTaskChild{" +
          "parentTaskId=" + parentTaskId +
          ", id=" + id +
          ", tenantId='" + tenantId + '\'' +
          ", username='" + username + '\'' +
          ", sourceSystemId='" + sourceSystemId + '\'' +
          ", sourcePath='" + sourcePath + '\'' +
          ", destinationSystemId='" + destinationSystemId + '\'' +
          ", destinationPath='" + destinationPath + '\'' +
          '}';
    }
}
