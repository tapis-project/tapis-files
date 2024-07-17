package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskChildDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskParentDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DefaultSchedulingPolicy implements SchedulingPolicy {
    Logger log = LoggerFactory.getLogger(DefaultSchedulingPolicy.class);
    private final int cachedRows;

    public DefaultSchedulingPolicy(int cachedRows) {
        this.cachedRows = cachedRows;
    }

    @Override
    public void assignChildTasksToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException {
        if(workerIds.size() == 0) {
            // if there are no workers in the list, there is nothing to do
            return;
        }

        int amountPerWorker = (int) Math.ceil((double)queuedTaskIds.size() / (double)workerIds.size());
        for (UUID uuid : workerIds) {
            List<Integer> tasksToAssign = new ArrayList<>();
            for(int i=0;i<amountPerWorker;i++) {
                if(i < queuedTaskIds.size()) {
                    tasksToAssign.add(queuedTaskIds.remove(i));
                } else {
                    break;
                }
            }
            assignChildTasksToWorker(tasksToAssign, uuid);
        }
    }

    @Override
    public List<PrioritizedObject<TransferTaskChild>> getChildTasksForWorker(UUID workerUuid) throws SchedulingPolicyException {
        // get all the work to be assigned
        List<PrioritizedObject<TransferTaskChild>> prioritizedWork = null;
        try {
            TransferTaskChildDAO childDao = new TransferTaskChildDAO();
            prioritizedWork = DAOTransactionContext.doInTransaction(context -> {
                return childDao.getAssignedWorkForWorker(context, cachedRows, workerUuid);
            });
        } catch (DAOException ex) {
            throw new SchedulingPolicyException(MsgUtils.getMsg("FILES_TXFR_DEFAULT_SCHEDULING_POLICY_ERROR", "getWorkForWorker", ex));
        }

        return prioritizedWork;
    }

    @Override
    public List<Integer> getQueuedChildTaskIds() throws SchedulingPolicyException {
        // get all the work to be assigned
        List<PrioritizedObject<TransferTaskChild>> prioritizedWork = null;
        try {
            TransferTaskChildDAO childDao = new TransferTaskChildDAO();
            prioritizedWork = DAOTransactionContext.doInTransaction(context -> {
                return childDao.getAcceptedChildTasksForTenantsAndUsers(context, cachedRows);
            });
        } catch (DAOException ex) {
            throw new SchedulingPolicyException(MsgUtils.getMsg("FILES_TXFR_DEFAULT_SCHEDULING_POLICY_ERROR", "getQueuedTaskIds", ex));
        }

        if(CollectionUtils.isEmpty(prioritizedWork)) {
            return Collections.emptyList();
        }

        List<Integer> queuedTaskIds = new ArrayList<>();
        Map<String, List<Integer>> tenantWorkMap = refillChildTasks(prioritizedWork);
        while(!tenantWorkMap.isEmpty()) {
            Iterator<String> tenantIterator = tenantWorkMap.keySet().iterator();
            while (tenantIterator.hasNext()) {
                String tenant = tenantIterator.next();
                List<Integer> childList = tenantWorkMap.get(tenant);
                if (childList.isEmpty()) {
                    tenantIterator.remove();
                } else {
                    queuedTaskIds.add(childList.remove(0));
                }
            }
        }

        return queuedTaskIds;
    }

    @Override
    public List<Integer> getQueuedParentTaskIds() throws SchedulingPolicyException {
        // get all the work to be assigned
        List<PrioritizedObject<TransferTaskParent>> prioritizedWork = null;
        try {
            TransferTaskParentDAO parentDAO = new TransferTaskParentDAO();
            prioritizedWork = DAOTransactionContext.doInTransaction(context -> {
                return parentDAO.getAcceptedParentTasksForTenantsAndUsers(context, cachedRows);
            });
        } catch (DAOException ex) {
            throw new SchedulingPolicyException(MsgUtils.getMsg("FILES_TXFR_DEFAULT_SCHEDULING_POLICY_ERROR", "getQueuedTaskIds", ex));
        }

        if(CollectionUtils.isEmpty(prioritizedWork)) {
            return Collections.emptyList();
        }

        List<Integer> queuedTaskIds = new ArrayList<>();
        Map<String, List<Integer>> tenantWorkMap = refillParentTasks(prioritizedWork);
        while(!tenantWorkMap.isEmpty()) {
            Iterator<String> tenantIterator = tenantWorkMap.keySet().iterator();
            while (tenantIterator.hasNext()) {
                String tenant = tenantIterator.next();
                List<Integer> parentList = tenantWorkMap.get(tenant);
                if (parentList.isEmpty()) {
                    tenantIterator.remove();
                } else {
                    queuedTaskIds.add(parentList.remove(0));
                }
            }
        }

        return queuedTaskIds;
    }

    @Override
    public void assignParentTasksToWorkers(List<UUID> workerIds, List<Integer> queuedTaskIds) throws SchedulingPolicyException {
        if(workerIds.size() == 0) {
            // if there are no workers in the list, there is nothing to do
            return;
        }

        int amountPerWorker = (int) Math.ceil((double)queuedTaskIds.size() / (double)workerIds.size());
        for (UUID uuid : workerIds) {
            List<Integer> tasksToAssign = new ArrayList<>();
            for(int i=0;i<amountPerWorker;i++) {
                if(i < queuedTaskIds.size()) {
                    tasksToAssign.add(queuedTaskIds.remove(i));
                } else {
                    break;
                }
            }
            assignParentTasksToWorker(tasksToAssign, uuid);
        }
    }

    @Override
    public List<PrioritizedObject<TransferTaskParent>> getParentTasksForWorker(UUID workerUuid) throws SchedulingPolicyException {
        // get all the work to be assigned
        List<PrioritizedObject<TransferTaskParent>> prioritizedWork = null;
        try {
            TransferTaskParentDAO parentDAO = new TransferTaskParentDAO();
            prioritizedWork = DAOTransactionContext.doInTransaction(context -> {
                return parentDAO.getAssignedWorkForWorker(context, cachedRows, workerUuid);
            });
        } catch (DAOException ex) {
            throw new SchedulingPolicyException(MsgUtils.getMsg("FILES_TXFR_DEFAULT_SCHEDULING_POLICY_ERROR", "getWorkForWorker", ex));
        }

        return prioritizedWork;

    }

    private void assignChildTasksToWorker(List<Integer> taskIds, UUID workerId) throws SchedulingPolicyException {
        try {
            TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
            DAOTransactionContext.doInTransaction(context -> {
                childTaskDAO.assignToWorkers(context, taskIds, workerId);
                context.commit();
                return null;
            });
        } catch (DAOException ex) {
            throw new SchedulingPolicyException(MsgUtils.getMsg("FILES_TXFR_DEFAULT_SCHEDULING_POLICY_ERROR", "assignTasksToWorker", ex));
        }

    }

    private void assignParentTasksToWorker(List<Integer> taskIds, UUID workerId) throws SchedulingPolicyException {
        try {
            TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
            DAOTransactionContext.doInTransaction(context -> {
                parentTaskDAO.assignToWorkers(context, taskIds, workerId);
                context.commit();
                return null;
            });
        } catch (DAOException ex) {
            throw new SchedulingPolicyException(MsgUtils.getMsg("FILES_TXFR_DEFAULT_SCHEDULING_POLICY_ERROR", "assignParentTasksToWorker", ex));
        }

    }

    private Map<String, List<Integer>> refillChildTasks(List<PrioritizedObject<TransferTaskChild>> prioritizedObjects) {
        Map<String, List<Integer>> returnMap = new HashMap<>();

        // put tenant/work in map in priority order
        for(var prioritizedWork : prioritizedObjects) {
            TransferTaskChild work = prioritizedWork.getObject();
            List<Integer> childList = returnMap.get(work.getTenantId());
            if(childList == null) {
                childList = new ArrayList<>();
                returnMap.put(work.getTenantId(), childList);
            }

           childList.add(prioritizedWork.getObject().getId());
        }

        return returnMap;
    }

    private Map<String, List<Integer>> refillParentTasks(List<PrioritizedObject<TransferTaskParent>> prioritizedObjects) {
        Map<String, List<Integer>> returnMap = new HashMap<>();

        // put tenant/work in map in priority order
        for(var prioritizedWork : prioritizedObjects) {
            TransferTaskParent work = prioritizedWork.getObject();
            List<Integer> parentList = returnMap.get(work.getTenantId());
            if(parentList == null) {
                parentList = new ArrayList<>();
                returnMap.put(work.getTenantId(), parentList);
            }

            parentList.add(prioritizedWork.getObject().getId());
        }

        return returnMap;
    }

}
