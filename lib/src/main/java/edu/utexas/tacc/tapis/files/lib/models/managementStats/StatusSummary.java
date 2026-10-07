package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.util.HashMap;
import java.util.Map;

public class StatusSummary {
    int inProgressTopTasks;

    int inProgressChildTasks;
    int assignedChildTasks;
    int unassignedChildTasks;
    int awaitingRetryChildTasks;

    int inProgressParentTasks;
    int assignedParentTasks;
    int unassignedParentTasks;
    int awaitingRetryParentTasks;

//    Map<String, Integer> assignmentsByWorker = new HashMap();

    public int getInProgressTopTasks() {
        return inProgressTopTasks;
    }

    public void setInProgressTopTasks(int inProgressTopTasks) {
        this.inProgressTopTasks = inProgressTopTasks;
    }

    public int getInProgressChildTasks() {
        return inProgressChildTasks;
    }

    public void setInProgressChildTasks(int inProgressChildTasks) {
        this.inProgressChildTasks = inProgressChildTasks;
    }

    public int getAssignedChildTasks() {
        return assignedChildTasks;
    }

    public void setAssignedChildTasks(int assignedChildTasks) {
        this.assignedChildTasks = assignedChildTasks;
    }

    public int getUnassignedChildTasks() {
        return unassignedChildTasks;
    }

    public void setUnassignedChildTasks(int unassignedChildTasks) {
        this.unassignedChildTasks = unassignedChildTasks;
    }

    public int getAwaitingRetryChildTasks() {
        return awaitingRetryChildTasks;
    }

    public void setAwaitingRetryChildTasks(int awaitingRetryChildTasks) {
        this.awaitingRetryChildTasks = awaitingRetryChildTasks;
    }

    public int getInProgressParentTasks() {
        return inProgressParentTasks;
    }

    public void setInProgressParentTasks(int inProgressParentTasks) {
        this.inProgressParentTasks = inProgressParentTasks;
    }

    public int getAssignedParentTasks() {
        return assignedParentTasks;
    }

    public void setAssignedParentTasks(int assignedParentTasks) {
        this.assignedParentTasks = assignedParentTasks;
    }

    public int getUnassignedParentTasks() {
        return unassignedParentTasks;
    }

    public void setUnassignedParentTasks(int unassignedParentTasks) {
        this.unassignedParentTasks = unassignedParentTasks;
    }

    public int getAwaitingRetryParentTasks() {
        return awaitingRetryParentTasks;
    }

    public void setAwaitingRetryParentTasks(int awaitingRetryParentTasks) {
        this.awaitingRetryParentTasks = awaitingRetryParentTasks;
    }

//    public Map<String, Integer> getAssignmentsByWorker() {
//        return assignmentsByWorker;
//    }
}
