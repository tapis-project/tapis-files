package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;

import java.util.List;
import java.util.UUID;

public interface SchedulingPolicy {
    /**
     * Gets the queued child task ids.  This is the queue containing the next batch of child tasks that need to be assigned
     * in the order that they should be assigned.
     *
     * @return the List of child task id's that need to be assigned in order that they should be assigned
     * @throws SchedulingPolicyException
     */
    List<Integer> getQueuedChildTaskIds() throws SchedulingPolicyException;

    /**
     * Does the actual assignment of child tasks to workers by setting the "assignedTo" field of the child task
     * @param workerIds
     * @param queuedTaskIds
     * @throws SchedulingPolicyException
     */
    void assignChildTasksToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException;

    /**
     * Gets the child tasks assigned to a worker in the order that they should be completed (used by the code that
     * will actually process the task)
     * @param workerUuid
     * @return
     * @throws SchedulingPolicyException
     */
    List<PrioritizedObject<TransferTaskChild>> getChildTasksForWorker(UUID workerUuid) throws SchedulingPolicyException;

    /**
     * Gets the queued parent task ids.  This is the queue containing the next batch of parent tasks that need to be assigned
     * in the order that they should be assigned.
     *
     * @return the List of child task id's that need to be assigned in order that they should be assigned
     * @throws SchedulingPolicyException
     */
    List<Integer> getQueuedParentTaskIds() throws SchedulingPolicyException;

    /**
     * Does the actual assignment of parent tasks to workers by setting the "assignedTo" field of the parent task
     * @param workerIds
     * @param queuedTaskIds
     * @throws SchedulingPolicyException
     */
    void assignParentTasksToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException;

    /**
     * Gets the parent tasks assigned to a worker in the order that they should be completed (used by the code that
     * will actually process the task)
     * @param workerUuid
     * @return
     * @throws SchedulingPolicyException
     */
    List<PrioritizedObject<TransferTaskParent>> getParentTasksForWorker(UUID workerUuid) throws SchedulingPolicyException;
}
