package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.uri.TapisUrl;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.UUID;

public class TransferTaskChild extends TransferTaskParent {

    private int parentTaskId;
    private int retries;

    public TransferTaskChild() {}

    public TransferTaskChild(String tenantId, String username,
                             String sourceURI, String destinationURI,
                             int parentTaskId) {
        this.tenantId = tenantId;
        this.username = username;
        this.sourceURI = sourceURI;
        this.destinationURI = destinationURI;
        this.uuid = UUID.randomUUID();
        this.status = TransferTaskStatus.ACCEPTED.name();
        this.parentTaskId = parentTaskId;
    }

    /**
     *  @param transferTaskParent The TransferTask from which to derive this child
     * @param fileInfo The path to the single file in the source system
     */
    public TransferTaskChild(@NotNull TransferTaskParent transferTaskParent, @NotNull FileInfo fileInfo) throws TapisException {

        TapisUrl sourceURL = TapisUrl.makeTapisUrl(transferTaskParent.getSourceURI());
        TapisUrl destURL = TapisUrl.makeTapisUrl(transferTaskParent.getDestinationURI());


        // Given a
        Path destPath = PathUtils.relativizePathsForTransfer(
            sourceURL.getPath(),
            fileInfo.getPath(),
            destURL.getPath()
        );

        TapisUrl newSourceURL = new TapisUrl(sourceURL.getHost(), sourceURL.getSystemId(), fileInfo.getPath());
        TapisUrl newDestURL = new TapisUrl(destURL.getHost(), destURL.getSystemId(), destPath.toString());

        this.setParentTaskId(transferTaskParent.getId());
        this.setTaskId(transferTaskParent.getTaskId());
        this.setSourceURI(newSourceURL.toString());
        this.setDestinationURI(newDestURL.toString());
        this.setStatus(TransferTaskStatus.ACCEPTED.name());
        this.setTenantId(transferTaskParent.getTenantId());
        this.setUsername(transferTaskParent.getUsername());
        this.setBytesTransferred(0L);
        this.setTotalBytes(fileInfo.getSize());
    }

    public int getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(int parentTaskId) {
        this.parentTaskId = parentTaskId;
    }

    public int getRetries() {return retries; }
    public void setRetries(int retries) {this.retries = retries;}

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

}
