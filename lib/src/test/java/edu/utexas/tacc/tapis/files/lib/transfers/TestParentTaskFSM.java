package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import org.statefulj.fsm.FSM;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test
public class TestParentTaskFSM {

    FSM<TransferTask> fsm = ParentTaskFSM.getFSM();

    @Test
    public void testAcceptedTransitions() throws Exception {
        TransferTask task = new TransferTask();
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.ACCEPTED.name());
        fsm.onEvent(task, TransfersFSMEvents.TO_STAGING.name());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.STAGING.name());
        fsm.onEvent(task, TransfersFSMEvents.TO_STAGED.name());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.STAGED.name());
        Assert.assertThrows(Exception.class, ()->{
            fsm.onEvent(task, "boo");
        });
    }

}