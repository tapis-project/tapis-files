package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;

import java.util.List;
import java.util.UUID;

public interface SchedulingPolicy {
    List<Integer> getQueuedChildTaskIds() throws SchedulingPolicyException;
    void assignChildTasksToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException;
    List<PrioritizedObject<TransferTaskChild>> getChildTasksForWorker(UUID workerUuid) throws SchedulingPolicyException;
    List<Integer> getQueuedParentTaskIds() throws SchedulingPolicyException;
    void assignParentTasksToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException;
    List<PrioritizedObject<TransferTaskParent>> getParentTasksForWorker(UUID workerUuid) throws SchedulingPolicyException;
}
