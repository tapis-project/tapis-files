package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import java.time.Instant;
import java.util.UUID;

public class ChildTaskInfo extends TaskInfo {
    private int parentTaskId;
    private int topTaskId;
    private UUID assignedTo;
    private int retriesRemaining;
    private Instant nextRetry;

    public static TaskInfo fromChildTask(TransferTaskChild task) {
        ChildTaskInfo childTaskInfo = new ChildTaskInfo(task.getId(), task.getUuid(), task.getStatus().toString(),
                task.getTaskId(), task.getParentTaskId(), task.getTenantId(), task.getUsername(),
                task.getRetriesRemaining(), task.getNextRetry(), task.getCreated());
        childTaskInfo.setErrorMessage(task.getErrorMessage());
        childTaskInfo.setAssignedTo(task.getAssignedTo());
        return childTaskInfo;
    }

    public ChildTaskInfo(int id, UUID uuid, String status, int topTaskId, int parentTaskId, String tenant, String user,
                         int retriesRemaining, Instant nextRetry, Instant created) {
        super(id, uuid, status, tenant, user, created);
        this.topTaskId = topTaskId;
        this.parentTaskId = parentTaskId;
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
