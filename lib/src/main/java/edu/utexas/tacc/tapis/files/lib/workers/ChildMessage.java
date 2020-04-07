package edu.utexas.tacc.tapis.files.lib.workers;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ChildMessage {

    private int parentTaskId;
    private int id;
    private final ObjectMapper mapper = new ObjectMapper();

    private ChildMessage() {}

    public ChildMessage(int parentTaskId, int id) {
        this.parentTaskId = parentTaskId;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public int getParentTaskId() {
        return parentTaskId;
    }

    public void setParentTaskId(int parentTaskId) {
        this.parentTaskId = parentTaskId;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "ChildMessage{" +
                "parentTaskId='" + parentTaskId + '\'' +
                ", id=" + id +
                '}';
    }
}
