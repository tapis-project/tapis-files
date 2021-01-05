package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
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
                             String sourceURI,
                             String destinationURI, int parentTaskId) {
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
    public TransferTaskChild(@NotNull TransferTaskParent transferTaskParent, @NotNull FileInfo fileInfo) {

        TapisUrl sourceUrl = TapisUrl.makeTapisUrl(transferTaskParent.getSourceURI());
        TapisUrl destURL = TapisUrl.makeTapisUrl(transferTaskParent.getDestinationURI());

        Path destPath = PathUtils.relativizePathsForTransfer(
            sourceUrl.getPath(),
            fileInfo.getPath(),
            destURL.getPath()
        );

        URI newDestUrl = URI.create()

        this.setParentTaskId(transferTaskParent.getId());
        this.setTaskId(transferTaskParent.getTaskId());
        this.setSourceSystemId(transferTaskParent.getSourceSystemId());
        this.setDestinationPath(destPath.toString());
        this.setDestinationSystemId(transferTaskParent.getDestinationSystemId());
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
