package edu.utexas.tacc.tapis.files.lib.transfers.actions;

import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.model.Action;

public class DoRootListing<T> implements Action<T> {

    private Logger log = LoggerFactory.getLogger(TransfersService.class);

    @Override
    public void execute(T stateful, String event, Object... args) throws RetryException {
        log.info("Doing root listing");
    }


}
