package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.util.ArrayList;
import java.util.List;

public class AssignerStats {
    private List<ParentTaskInfo> queuedParentTasks = new ArrayList<>();
    private List<ParentTaskInfo> parentTasksAwaitingRetry = new ArrayList<>();
    private List<AssignedParentTaskInfo> assignedParentTasks = new ArrayList<>();

    public List<ParentTaskInfo> getQueuedParentTasks() {
        return queuedParentTasks;
    }

    public List<AssignedParentTaskInfo> getAssignedParentTasks() {
        return assignedParentTasks;
    }

    public List<ParentTaskInfo> getParentTasksAwaitingRetry() {
        return parentTasksAwaitingRetry;
    }
}
