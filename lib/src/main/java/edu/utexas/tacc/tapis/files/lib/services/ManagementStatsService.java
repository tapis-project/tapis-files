package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.stats.ManagementStatsDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferWorkerDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.AssignedParentTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.AssignerStats;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ParentTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TransferWorkerInfo;
import edu.utexas.tacc.tapis.files.lib.transfers.DefaultSchedulingPolicy;
import edu.utexas.tacc.tapis.files.lib.transfers.SchedulingPolicy;
import org.jvnet.hk2.annotations.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class ManagementStatsService {
    public static final int CACHED_ROWS = 300;
    SchedulingPolicy schedulingPolicy = new DefaultSchedulingPolicy(CACHED_ROWS);

    public AssignerStats getAssignerStats() throws ServiceException {
        AssignerStats assignerStats = new AssignerStats();
        try {
            List<Integer> queuedParentTaskIds = schedulingPolicy.getQueuedParentTaskIds();

            Set<TransferWorkerInfo> transferWorkers = getTransferWorkers();

            assignerStats.getQueuedParentTasks().addAll(getQueuedParentTasks(queuedParentTaskIds));
            assignerStats.getParentTasksAwaitingRetry().addAll(getParentTasksWaitingForRetry());
/*
            for (TransferWorkerInfo transferWorker : transferWorkers) {
                schedulingPolicy.getParentTasksForWorker(UUID.fromString(transferWorker.getUuid())).stream().forEach(prioritizedParentTask -> {
                    TransferTaskParent ttp = prioritizedParentTask.getObject();
                    assignerStats.getAssignedParentTasks().add(
                            AssignedParentTaskInfo.fromParentTask(prioritizedParentTask.getPriority(), ttp));
                });
            }

 */
            assignerStats.getAssignedParentTasks().addAll(getAssignedParentTasks(transferWorkers));
        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }

        return assignerStats;
    }

    private List<AssignedParentTaskInfo> getAssignedParentTasks(Set<TransferWorkerInfo> transferWorkers) throws SchedulingPolicyException {
//        List<AssignedParentTaskInfo> assignedParentTaskInfos = new ArrayList<>();
//        for (TransferWorkerInfo transferWorker : transferWorkers) {
//            schedulingPolicy.getParentTasksForWorker(UUID.fromString(transferWorker.getUuid())).stream().forEach(prioritizedParentTask -> {
//                TransferTaskParent ttp = prioritizedParentTask.getObject();
//                assignedParentTaskInfos.add(
//                        AssignedParentTaskInfo.fromParentTask(prioritizedParentTask.getPriority(), ttp));
//            });
//        }
//        return assignedParentTaskInfos;
        return transferWorkers.stream().flatMap(transferWorker -> {
            try {
                return getAssignedParentTasksForWorker(transferWorker).stream();
            } catch (SchedulingPolicyException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }
    private List<AssignedParentTaskInfo> getAssignedParentTasksForWorker(TransferWorkerInfo transferWorker) throws SchedulingPolicyException {
        List<AssignedParentTaskInfo> assignedParentTaskInfos = schedulingPolicy.getParentTasksForWorker(
                UUID.fromString(transferWorker.getUuid())).stream().map(prioritizedParentTask -> {
            TransferTaskParent ttp = prioritizedParentTask.getObject();
            return AssignedParentTaskInfo.fromParentTask(prioritizedParentTask.getPriority(), ttp);
        }).toList();
        return assignedParentTaskInfos;
    }

    private Set<TransferWorkerInfo> getTransferWorkers() throws DAOException {
        TransferWorkerDAO twDao = new TransferWorkerDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return twDao.getTransferWorkers(tx);
        }).stream().map(transferWorker -> {
            TransferWorkerInfo twInfo = new TransferWorkerInfo();
            twInfo.setUuid(transferWorker.getUuid().toString());
            twInfo.setLast_updated(transferWorker.getLastUpdated());
            return twInfo;
        }).collect(Collectors.toSet());
    }

    private List<ParentTaskInfo> getQueuedParentTasks(List<Integer> queuedParentTaskIds) throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getParentTasks(tx, queuedParentTaskIds);
        });
    }

    private List<ParentTaskInfo> getParentTasksWaitingForRetry() throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getParentTasksInStatus(tx, List.of(TransferTaskStatus.AWAITING_RETRY));
        });
    }
}
