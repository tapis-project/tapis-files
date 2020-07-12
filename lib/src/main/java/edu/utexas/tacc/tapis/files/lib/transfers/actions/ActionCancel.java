package edu.utexas.tacc.tapis.files.lib.transfers.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.model.Action;

public class ActionCancel<T> implements Action<T> {

    private static final Logger log = LoggerFactory.getLogger(ActionCancel.class);

    @Override
    public void execute(T stateful, String event, Object... args) throws RetryException {
        log.info("ActionCancel");
    }


}
