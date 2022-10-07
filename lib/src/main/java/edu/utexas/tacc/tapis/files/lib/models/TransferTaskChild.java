package edu.utexas.tacc.tapis.files.lib.models;

import java.nio.file.Path;
import java.util.StringJoiner;
import java.util.UUID;
import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public class TransferTaskChild extends TransferTaskParent
{
  private int parentTaskId;
  private int retries;
  private boolean isDir;

  public TransferTaskChild() {}

  public TransferTaskChild(String tenantId1, String username1, TransferURI srcUri1, TransferURI dstUri1, int parentId1)
  {
    tenantId = tenantId1;
    username = username1;
    sourceURI = srcUri1;
    destinationURI = dstUri1;
    uuid = UUID.randomUUID();
    status = TransferTaskStatus.ACCEPTED;
    parentTaskId = parentId1;
  }

  /**
   * @param transferTaskParent The TransferTask from which to derive this child
   * @param fileInfo The path to the single file in the source system
   */
  public TransferTaskChild(@NotNull TransferTaskParent transferTaskParent, @NotNull FileInfo fileInfo,
                           @NotNull TapisSystem sourceSystem)
          throws TapisException
  {
    TransferURI sourceUri = transferTaskParent.getSourceURI();
    TransferURI destUri = transferTaskParent.getDestinationURI();

    // TODO/TBD: Using this appears to be the cause of a bug when the source system is of type S3
    String destPathStr;
    // If the source system is of type S3 then we should not call relativizePaths because we do not support directory transfer
    if (sourceSystem != null && SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      // Ignore source path. Always create  file or object at destination path.
//      runTxfr(testSystemS3a, "a/b/file1.txt", testSystemSSHa, "ssha/s3a_txfr/file_from_s3a.txt", 1, clientSSHa);
      // TODO How best to construct the destination path? The source is an S3 object that in principal might not look
      //      anything like a path. What do we want to create a destination? Does it change if destination is S3 vs Linux? probably
      //      Or maybe we do not care what source "file" name is. We should take the destination path as a posix file or S3 key and create it
      destPathStr = destUri.getPath();
    }
    else
    {
      destPathStr = PathUtils.relativizePaths(sourceUri.getPath(), fileInfo.getPath(), destUri.getPath()).toString();
    }

    TransferURI newSourceUri = new TransferURI(sourceUri, fileInfo.getPath());
    TransferURI newDestUri = new TransferURI(destUri, destPathStr);

    setParentTaskId(transferTaskParent.getId());
    setTaskId(transferTaskParent.getTaskId());
    setSourceURI(newSourceUri.toString());
    setDestinationURI(newDestUri.toString());
    setStatus(TransferTaskStatus.ACCEPTED.name());
    setTenantId(transferTaskParent.getTenantId());
    setUsername(transferTaskParent.getUsername());
    setBytesTransferred(0L);
    setTotalBytes(fileInfo.getSize());
    setDir(fileInfo.isDir());
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

  public int getParentTaskId() { return parentTaskId; }
  public void setParentTaskId(int i) { parentTaskId = i; }
  public int getRetries() { return retries; }
  public void setRetries(int i) { retries = i; }
  public boolean isDir() { return isDir; }
  public void setDir(boolean b) { isDir = b; }
}
