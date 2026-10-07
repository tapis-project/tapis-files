package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;

import java.time.Instant;
import java.util.UUID;

public class AssignedParentTaskInfo extends ParentTaskInfo {
    private String workerUUID;
    private int priority;
    public static AssignedParentTaskInfo fromParentTask(int priority, TransferTaskParent task) {
        AssignedParentTaskInfo assignedParentTaskInfo = new AssignedParentTaskInfo(priority, task.getId(),
                task.getUuid(), task.getStatus().toString(), task.getTaskId(), task.getTenantId(),
                task.getUsername(), task.getRetriesRemaining(), task.getNextRetry(), task.getCreated());
        assignedParentTaskInfo.setErrorMessage(task.getErrorMessage());
        assignedParentTaskInfo.setAssignedTo(task.getAssignedTo());
        return assignedParentTaskInfo;
    }

    public AssignedParentTaskInfo(int priority, int id, UUID uuid, String status, int topTaskId, String tenant,
                                  String user, int retriesRemaining, Instant nextRetry, Instant created) {
        super(id, uuid, status, topTaskId, tenant, user, retriesRemaining, nextRetry, created);
        this.priority = priority;
    }

    public String getWorkerUUID() {
        return workerUUID;
    }

    public void setWorkerUUID(String workerUUID) {
        this.workerUUID = workerUUID;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
}
