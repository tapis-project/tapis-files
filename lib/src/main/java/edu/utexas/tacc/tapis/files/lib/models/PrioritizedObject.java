package edu.utexas.tacc.tapis.files.lib.models;

public class PrioritizedObject <T> {
    private int priority;
    private T object;
    public PrioritizedObject(int priority, T object) {
        this.priority = priority;
        this.object = object;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public T getObject() {
        return object;
    }

    public void setObject(T object) {
        this.object = object;
    }
}
