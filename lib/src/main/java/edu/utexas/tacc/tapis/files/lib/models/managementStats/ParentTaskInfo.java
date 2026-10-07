package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.time.Instant;
import java.util.UUID;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;

public class ParentTaskInfo extends TaskInfo {
    private int topTaskId;
    private int retriesRemaining;
    private Instant nextRetry;
    private UUID assignedTo;
    public static ParentTaskInfo frtomParentTask(TransferTaskParent task) {
        ParentTaskInfo parentTaskInfo = new ParentTaskInfo(task.getId(), task.getUuid(), task.getStatus().toString(),
                task.getTaskId(), task.getTenantId(), task.getUsername(), task.getRetriesRemaining(),
                task.getNextRetry(), task.getCreated());
        parentTaskInfo.setErrorMessage(task.getErrorMessage());
        parentTaskInfo.setAssignedTo(task.getAssignedTo());
        return parentTaskInfo;
    }

    public ParentTaskInfo(int id, UUID uuid, String status, int topTaskId, String tenant, String user, int retriesRemaining,
                          Instant nextRetry, Instant created) {
        super(id, uuid, status, tenant, user, created);
        this.topTaskId = topTaskId;
        this.retriesRemaining = retriesRemaining;
        this.nextRetry = nextRetry;
    }

    public void setAssignedTo(UUID assignedTo) {
        this.assignedTo = assignedTo;
    }

    public int getRetriesRemaining() {
        return retriesRemaining;
    }

    public UUID getAssignedTo() {
        return assignedTo;
    }

}
