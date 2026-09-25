package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;

public class ParentTaskInfo extends TaskInfo {
    private int topTaskId;
    private int retriesRemaining;
    private Optional<Instant> nextRetry;
    private Optional<UUID> assignedTo;
    public static ParentTaskInfo fromParentTask(TransferTaskParent task) {
        ParentTaskInfo parentTaskInfo = new ParentTaskInfo(task.getId(), task.getStatus().toString(), task.getTaskId(),
                task.getTenantId(), task.getUsername(), task.getRetriesRemaining(), task.getNextRetry(), task.getCreated());
        parentTaskInfo.setErrorMessage(task.getErrorMessage());
        parentTaskInfo.setAssignedTo(task.getAssignedTo());
        return parentTaskInfo;
    }

    public ParentTaskInfo(int id, String status, int topTaskId, String tenant, String user, int retriesRemaining,
                          Instant nextRetry, Instant created) {
        super(id, status, tenant, user, created);
        this.topTaskId = topTaskId;
        this.retriesRemaining = retriesRemaining;
        this.nextRetry = (nextRetry == null) ? Optional.empty() : Optional.of(nextRetry);
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
