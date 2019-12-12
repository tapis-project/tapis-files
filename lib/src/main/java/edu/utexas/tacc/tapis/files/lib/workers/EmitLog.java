package edu.utexas.tacc.tapis.files.lib.workers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;

public class EmitLog {

    public static void main(String[] argv) throws Exception {

        for (var i=0; i<10; i++) {
            TransferTask task = new TransferTask();
            task.setUsername("test");
            task.setTenantId("testTenant");
            task.setSourceSystemId("sourceSystem");
            task.setSourcePath("sourcePath");
            task.setDestinationSystemId("destinationSystem");
            task.setDestinationPath("destinationPath");
            TransfersService transfersService = new TransfersService();
            transfersService.publishTransferTaskMessage(task);
            System.out.println(" [x] Sent '" + "");
        }
    }
}