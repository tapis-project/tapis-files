package edu.utexas.tacc.tapis.files.lib.models;

import javax.validation.constraints.NotNull;
import java.math.BigInteger;
import java.time.Instant;

public class TransferTaskChild extends TransferTask{

    private int parentTaskId;

    public TransferTaskChild(){};

    public TransferTaskChild(@NotNull TransferTask transferTask, @NotNull String sourcePath) {
        this.setSourcePath(sourcePath);
        this.setSourceSystemId(transferTask.getSourceSystemId());
        this.setParentTaskId(transferTask.getId());
        this.setDestinationPath(transferTask.getDestinationPath());
        this.setDestinationSystemId(transferTask.getDestinationSystemId());
        this.setStatus(TransferTaskStatus.ACCEPTED);
        this.setTenantId(transferTask.getTenantId());
        this.setUsername(transferTask.getUsername());
        this.setBytesTransferred(0L);
        this.setTotalBytes(0L);
    }

    public int getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(int parentTaskId) {
        this.parentTaskId = parentTaskId;
    }
}
