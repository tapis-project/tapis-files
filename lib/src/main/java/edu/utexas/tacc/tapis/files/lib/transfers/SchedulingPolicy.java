package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;

import java.util.List;
import java.util.UUID;

public interface SchedulingPolicy {
    List<Integer> getQueuedTaskIds() throws SchedulingPolicyException;
    void assignWorkToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException;
    List<PrioritizedObject<TransferTaskChild>> getWorkForWorker(UUID workerUuid) throws SchedulingPolicyException;
}
