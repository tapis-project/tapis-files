package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.statefulj.fsm.FSM;
import org.statefulj.fsm.Persister;
import org.statefulj.fsm.RetryException;
import org.statefulj.fsm.TooBusyException;
import org.statefulj.fsm.model.State;
import org.statefulj.fsm.model.Transition;
import org.statefulj.fsm.model.impl.StateImpl;
import org.statefulj.persistence.memory.MemoryPersisterImpl;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;

public class ParentTaskFSM<T extends ITransfersFSMStatefulEntity> extends FSM<T> {

    public ParentTaskFSM(String name, Persister<T> persister) {
        super(name, persister);
    }

    private final State<TransferTask> ACCEPTED = new StateImpl<>(TransferTaskStatus.ACCEPTED.name());
    private final State<TransferTask> INPROGRESS = new StateImpl<>(TransferTaskStatus.IN_PROGRESS.name());
    private final State<TransferTask> STAGED = new StateImpl<>(TransferTaskStatus.STAGED.name());
    private final State<TransferTask> PAUSED = new StateImpl<>(TransferTaskStatus.PAUSED.name());
    private final State<TransferTask> CANCELLED = new StateImpl<>(TransferTaskStatus.CANCELLED.name(), true);
    private final State<TransferTask> FAILED = new StateImpl<>(TransferTaskStatus.FAILED.name(), true);
    private final State<TransferTask> COMPLETED = new StateImpl<>(TransferTaskStatus.COMPLETED.name(), true);
    private final List<State<TransferTask>> states = new LinkedList<>();

    @PostConstruct
    private void init() {
        states.add(ACCEPTED);
        states.add(STAGED);
        states.add(INPROGRESS);
        states.add(COMPLETED);
        states.add(CANCELLED);
        states.add(FAILED);
        states.add(PAUSED);

        ACCEPTED.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED);
        ACCEPTED.addTransition(TransfersFSMEvents.TO_STAGED.name(), STAGED);
        ACCEPTED.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED);
        ACCEPTED.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED);

        // STAGED state means that the initial listing is complete and all records are in DB and in
        // a queue
        STAGED.addTransition(TransfersFSMEvents.TO_INPROGRESS.name(), INPROGRESS);
        STAGED.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED);
        STAGED.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED);
        STAGED.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED);


        INPROGRESS.addTransition(TransfersFSMEvents.TO_COMPLETED.name(), COMPLETED);
        INPROGRESS.addTransition(TransfersFSMEvents.TO_FAILED.name(), FAILED);
        INPROGRESS.addTransition(TransfersFSMEvents.TO_CANCELLED.name(), CANCELLED);
        INPROGRESS.addTransition(TransfersFSMEvents.TO_PAUSED.name(), PAUSED);

        CANCELLED.addTransition(TransfersFSMEvents.TO_INPROGRESS.name(), INPROGRESS);
        FSM<TransferTask> transfersFSM = new FSM<>(new MemoryPersisterImpl<>(states, ACCEPTED));

    }





    @Override
    public State<T> onEvent(T stateful, String event, Object... args) throws IllegalStateException {
        // Attempt to transition to a new state given the current state
        // and an event.
        State<T> current = this.getCurrentState(stateful);

        // Fetch the transition for this event from the current state
        Transition<T> transition = this.getTransition(event, current);
        try {
            current = this.transition(stateful, current, event, transition, args);
            return current;
        } catch (RetryException e) {
            String msg = "ERROR: ParentTransfer rety error";
            throw new IllegalStateException(msg, e);
        }
    }
}
