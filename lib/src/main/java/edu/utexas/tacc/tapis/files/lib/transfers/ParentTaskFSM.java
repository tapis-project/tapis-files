package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.transfers.actions.*;
import org.jvnet.hk2.annotations.Service;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.Persister;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.TooBusyException;
import org.statefulj.fsm.model.Action;
import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.Transition;
import org.statefulj.fsm.model.impl.StateImpl;
import org.statefulj.persistence.memory.MemoryPersisterImpl;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;

@Service
public class ParentTaskFSM {

    private static final State<TransferTask> ACCEPTED = new StateImpl<>(TransferTaskStatus.ACCEPTED.name());
    private static final State<TransferTask> STAGING = new StateImpl<>(TransferTaskStatus.STAGING.name());
    private static final State<TransferTask> STAGED = new StateImpl<>(TransferTaskStatus.STAGED.name());
    private static final State<TransferTask> INPROGRESS = new StateImpl<>(TransferTaskStatus.IN_PROGRESS.name());
    private static final State<TransferTask> PAUSED = new StateImpl<>(TransferTaskStatus.PAUSED.name());
    private static final State<TransferTask> CANCELLED = new StateImpl<>(TransferTaskStatus.CANCELLED.name(), true);
    private static final State<TransferTask> FAILED = new StateImpl<>(TransferTaskStatus.FAILED.name(), true);
    private static final State<TransferTask> COMPLETED = new StateImpl<>(TransferTaskStatus.COMPLETED.name(), true);
    private static final List<State<TransferTask>> states = new LinkedList<>();

    private static FSM<TransferTask> fsm;

    public static FSM<TransferTask> getFSM() {
        if (fsm == null) {
            states.add(ACCEPTED);
            states.add(STAGING);
            states.add(STAGED);
            states.add(PAUSED);
            states.add(INPROGRESS);
            states.add(COMPLETED);
            states.add(CANCELLED);
            states.add(FAILED);


            Action<TransferTask> actionStage = new ActionStage<>();
            Action<TransferTask> actionFail = new ActionFail<>();
            Action<TransferTask> actionInProgress = new ActionInProgress<>();
            Action<TransferTask> actionPause = new ActionPause<>();
            Action<TransferTask> actionCancel = new ActionCancel<>();
            Action<TransferTask> actionStaging = new ActionStaging<>();
            Action<TransferTask> actionComplete = new ActionComplete<>();

            ACCEPTED.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED, actionCancel);
            ACCEPTED.addTransition(TransfersFSMEvents.TO_STAGING.name(), STAGING, actionStaging);
            ACCEPTED.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED, actionFail);
            ACCEPTED.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED, actionPause);

            // STAGING means that the listing is in progress, but has not completed
            STAGING.addTransition(TransfersFSMEvents.TO_STAGED.name(), STAGED, actionStage);
            STAGING.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED, actionFail);
            STAGING.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED, actionCancel);
            STAGING.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED, actionPause);

            // STAGED state means that the initial listing is complete and all records are in DB and in
            // a queue
            STAGED.addTransition(TransfersFSMEvents.TO_INPROGRESS.name(), INPROGRESS, actionInProgress);
            STAGED.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED, actionCancel);
            STAGED.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED, actionFail);
            STAGED.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED, actionPause);


            INPROGRESS.addTransition(TransfersFSMEvents.TO_COMPLETED.name(), COMPLETED, actionComplete);
            INPROGRESS.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED, actionFail);
            INPROGRESS.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED, actionCancel);
            INPROGRESS.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED, actionPause);

            PAUSED.addTransition(TransfersFSMEvents.TO_INPROGRESS.name(), INPROGRESS, actionInProgress);
            PAUSED.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED, actionFail);

            //Init the fsm with the ACCEPTED STATE
            fsm = new FSM<>(new MemoryPersisterImpl<>(states, ACCEPTED));
        }
        return fsm;
    }


}
