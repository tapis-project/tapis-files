package edu.utexas.tacc.tapis.files.lib.models;

import java.time.Instant;

public class TransferControlAction {

    public static enum ControlAction {
        CANCEL,
        PAUSE
    }

    private String tenantId;
    private ControlAction action;
    private int taskId;
    private Instant created;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }


    public ControlAction getAction() {
        return action;
    }

    public void setAction(ControlAction action) {
        this.action = action;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    @Override
    public String toString() {
        return "TransferControlAction{" +
            "action=" + action +
            ", taskId=" + taskId +
            ", created=" + created +
            '}';
    }
}
