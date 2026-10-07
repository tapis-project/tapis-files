package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import java.time.Instant;

public class AssignedChildTaskInfo extends ChildTaskInfo {
    private String workerUUID;
    private int priority;
    public static AssignedChildTaskInfo fromChildTask(int priority, TransferTaskChild task) {
        AssignedChildTaskInfo assignedChildTaskInfo = new AssignedChildTaskInfo(priority, task.getId(),
                task.getStatus().toString(), task.getTaskId(), task.getParentTaskId(), task.getTenantId(), task.getUsername(),
                task.getRetriesRemaining(), task.getNextRetry(), task.getCreated());
        assignedChildTaskInfo.setErrorMessage(task.getErrorMessage());
        assignedChildTaskInfo.setAssignedTo(task.getAssignedTo());
        return assignedChildTaskInfo;
    }

    public AssignedChildTaskInfo(int priority, int id, String status, int topTaskId, int parentTaskId,
                                 String tenant, String user, int retriesRemaining, Instant nextRetry,
                                 Instant created) {
        super(id, status, topTaskId, parentTaskId, tenant, user, retriesRemaining, nextRetry, created);
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
