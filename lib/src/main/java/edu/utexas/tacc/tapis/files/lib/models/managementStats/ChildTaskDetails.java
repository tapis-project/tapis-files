package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.util.ArrayList;
import java.util.List;

public class ChildTaskDetails {
    // child
    private List<ChildTaskInfo> queuedChildTasks = new ArrayList<>();
    private List<ChildTaskInfo> childTasksAwaitingRetry = new ArrayList<>();
    private List<ChildTaskInfo> childInProgress = new ArrayList<>();
    private List<AssignedChildTaskInfo> assignedChildTasks = new ArrayList<>();


    public List<ChildTaskInfo> getQueuedChildTasks() {
        return queuedChildTasks;
    }
    public List<ChildTaskInfo> getChildTasksAwaitingRetry() {
        return childTasksAwaitingRetry;
    }
    public List<AssignedChildTaskInfo> getAssignedChildTasks() {
        return assignedChildTasks;
    }
    public List<ChildTaskInfo> getChildInProgress() {
        return childInProgress;
    }

}
