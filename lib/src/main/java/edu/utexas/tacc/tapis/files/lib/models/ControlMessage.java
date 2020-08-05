package edu.utexas.tacc.tapis.files.lib.models;

import java.util.UUID;

public class ControlMessage {

    private UUID parentTaskUUID;
    private String action;

    public UUID getParentTaskUUID() {
        return parentTaskUUID;
    }

    public void setParentTaskUUID(UUID parentTaskUUID) {
        this.parentTaskUUID = parentTaskUUID;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }


    @Override
    public String toString() {
        return "ControlMessage{" +
            "parentTaskUUID=" + parentTaskUUID +
            ", action='" + action + '\'' +
            '}';
    }

}
