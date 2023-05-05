package edu.utexas.tacc.tapis.files.lib.models;

import java.util.StringJoiner;
import java.util.UUID;
import javax.validation.constraints.NotNull;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public class TransferTaskChild extends TransferTaskParent
{
  private int parentTaskId;
  private int retries;
  private boolean isDir;
  private String externalTaskId = ""; // Id for an external async txfr, such as Globus

  /* *********************************************************************** */
  /*            Constructors                                                 */
  /* *********************************************************************** */

  public TransferTaskChild() {}

  public TransferTaskChild(String tenantId1, String username1, TransferURI srcUri1, TransferURI dstUri1, int parentId1,
                           String tag1, String externalTaskId1)
  {
    tenantId = tenantId1;
    username = username1;
    sourceURI = srcUri1;
    destinationURI = dstUri1;
    uuid = UUID.randomUUID();
    status = TransferTaskStatus.ACCEPTED;
    parentTaskId = parentId1;
    tag = tag1;
    externalTaskId = (externalTaskId1==null) ? "" : externalTaskId1;
  }

  /**
   * Constructor
   * @param transferTaskParent The TransferTask from which to derive this child
   * @param fileInfo The path to the single file in the source system
   */
  public TransferTaskChild(@NotNull TransferTaskParent transferTaskParent, @NotNull FileInfo fileInfo,
                           @NotNull TapisSystem sourceSystem)
          throws TapisException
  {
    TransferURI sourceUri = transferTaskParent.getSourceURI();
    TransferURI destUri = transferTaskParent.getDestinationURI();

    // Build the destination path
    String destPathStr;
    // To support proper creation of directories at target we need to relativize the paths.
    // TODO: E.g. if file1, file2 are on source at absolute path /a/b, and we are transferring to /c/b
    destPathStr = PathUtils.relativizePaths(sourceUri.getPath(), fileInfo.getPath(), destUri.getPath()).toString();

    // Create source and destination URIs
    TransferURI newSourceUri = new TransferURI(sourceUri, fileInfo.getPath());
    TransferURI newDestUri = new TransferURI(destUri, destPathStr);

    // Set attributes for child we are constructing.
    externalTaskId = "";
    setTag(transferTaskParent.getTag());
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
            .add("tag=" + tag)
            .add("parentTaskId=" + parentTaskId)
            .add("taskId=" + taskId)
            .add("externalTaskId=" + externalTaskId)
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
  public String getExternalTaskId() { return (externalTaskId==null) ? "" : externalTaskId; }
  public void setExternalTaskId(String s) { externalTaskId = (s==null) ? "" : s; }
}
