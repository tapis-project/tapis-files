package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class ChildTaskInfo extends TaskInfo {
    private int parentTaskId;
    private int topTaskId;
    private Optional<UUID> assignedTo;
    private int retriesRemaining;
    public static TaskInfo fromChildTask(TransferTaskChild task) {
        ChildTaskInfo childTaskInfo = new ChildTaskInfo(task.getId(), task.getStatus().toString(), task.getTaskId(),
                task.getParentTaskId(), task.getTenantId(), task.getUsername(), task.getRetriesRemaining(), task.getCreated());
        childTaskInfo.setErrorMessage(task.getErrorMessage());
        childTaskInfo.setAssignedTo(task.getAssignedTo());
        return childTaskInfo;
    }

    public ChildTaskInfo(int id, String status, int topTaskId, int parentTaskId, String tenant, String user, int retriesRemaining, Instant created) {
        super(id, status, tenant, user, created);
        this.topTaskId = topTaskId;
        this.parentTaskId = parentTaskId;
        this.retriesRemaining = retriesRemaining;
        this.assignedTo = Optional.empty();
    }

    public void setAssignedTo(UUID assignedTo) {
        if(assignedTo == null) {
            this.assignedTo = Optional.empty();
        } else {
            this.assignedTo = Optional.of(assignedTo);
        }
    }

    public int getRetriesRemaining() {
        return retriesRemaining;
    }

    @Override
    public Optional<String> getErrorMessage() {
        return super.getErrorMessage();
    }

    public Optional<UUID> getAssignedTo() {
        return assignedTo;
    }
}
