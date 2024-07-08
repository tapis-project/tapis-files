package edu.utexas.tacc.tapis.files.lib.exceptions;

public class SchedulingPolicyException extends Exception {
    public SchedulingPolicyException() {
        super();
    }

    public SchedulingPolicyException(String msg) {
        super(msg);
    }

    public SchedulingPolicyException(Throwable th) {
        super(th);
    }

    public SchedulingPolicyException(String msg, Throwable th) {
        super(msg, th);
    }

}
