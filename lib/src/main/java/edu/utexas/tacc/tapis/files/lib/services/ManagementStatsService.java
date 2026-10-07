package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.stats.ManagementStatsDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferWorkerDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.AssignedChildTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.AssignedParentTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ChildTaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ConnectionDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ParentTaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.StatusSummary;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ChildTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ParentTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TopTaskDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TopTaskInfo;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.TransferWorkerInfo;
import edu.utexas.tacc.tapis.files.lib.transfers.DefaultSchedulingPolicy;
import edu.utexas.tacc.tapis.files.lib.transfers.SchedulingPolicy;
import org.jvnet.hk2.annotations.Service;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class ManagementStatsService {
    public static final int CACHED_ROWS = 300;
    SchedulingPolicy schedulingPolicy = new DefaultSchedulingPolicy(CACHED_ROWS);
/*
    public TaskStats getTasksStats() throws ServiceException {
        TaskStats tasksStats = new TaskStats();
        try {
            Set<TransferWorkerInfo> transferWorkers = getTransferWorkers();

            List<Integer> queuedParentTaskIds = schedulingPolicy.getQueuedParentTaskIds();
            tasksStats.getQueuedParentTasks().addAll(getQueuedParentTasks(queuedParentTaskIds));
            tasksStats.getParentTasksAwaitingRetry().addAll(getParentTasksWaitingForRetry());
            tasksStats.getAssignedParentTasks().addAll(getAssignedParentTasks(transferWorkers));
            tasksStats.getParentInProgress().addAll(getParentInProgressTasks());

            List<Integer> queuedChildTaskIds = schedulingPolicy.getQueuedChildTaskIds();
            tasksStats.getQueuedChildTasks().addAll(getQueuedChildTasks(queuedChildTaskIds));
            tasksStats.getChildTasksAwaitingRetry().addAll(getChildTasksWaitingForRetry());
            tasksStats.getAssignedChildTasks().addAll(getAssignedChildTasks(transferWorkers));
            tasksStats.getChildInProgress().addAll(getChildInProgressTasks());

            tasksStats.getTopInProgress().addAll(getTopInProgressTasks());

        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }

        return tasksStats;
    }
*/
    public StatusSummary getStatusSummary() throws ServiceException {
        ManagementStatsDAO msDAO = new ManagementStatsDAO();
        StatusSummary statusSummary;
        try {
            statusSummary = DAOTransactionContext.doInTransaction(tx -> {
                return msDAO.getSummary(tx);
            });
        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }

        return statusSummary;
    }

    public TopTaskDetails getTopTaskDetails(int successLimit, int errorLimit) throws ServiceException {
        TopTaskDetails topTaskDetails = new TopTaskDetails();
        try {
            topTaskDetails.getTopInProgress().addAll(getTopInProgressTasks());
            topTaskDetails.getRecentSuccesses().addAll(getTopTaskRecentSuccesses(successLimit));
            topTaskDetails.getRecentErrors().addAll(getTopTaskRecentErrors(errorLimit));
        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }

        return topTaskDetails;
    }

    public ParentTaskDetails getParentTaskDetails() throws ServiceException {
        ParentTaskDetails parentTaskDetails = new ParentTaskDetails();
        try {
            Set<TransferWorkerInfo> transferWorkers = getTransferWorkers();

            List<Integer> queuedParentTaskIds = schedulingPolicy.getQueuedParentTaskIds();
            parentTaskDetails.getQueuedParentTasks().addAll(getQueuedParentTasks(queuedParentTaskIds));
            parentTaskDetails.getParentTasksAwaitingRetry().addAll(getParentTasksWaitingForRetry());
            parentTaskDetails.getAssignedParentTasks().addAll(getAssignedParentTasks(transferWorkers));
            parentTaskDetails.getParentInProgress().addAll(getParentInProgressTasks());

        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }

        return parentTaskDetails;
    }

    public ChildTaskDetails getChildTaskDetails() throws ServiceException {
        ChildTaskDetails childTaskDetails = new ChildTaskDetails();
        try {
            Set<TransferWorkerInfo> transferWorkers = getTransferWorkers();

            List<Integer> queuedChildTaskIds = schedulingPolicy.getQueuedChildTaskIds();
            childTaskDetails.getQueuedChildTasks().addAll(getQueuedChildTasks(queuedChildTaskIds));
            childTaskDetails.getChildTasksAwaitingRetry().addAll(getChildTasksWaitingForRetry());
            childTaskDetails.getAssignedChildTasks().addAll(getAssignedChildTasks(transferWorkers));
            childTaskDetails.getChildInProgress().addAll(getChildInProgressTasks());
        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }

        return childTaskDetails;
    }

    public TaskDetails getTaskDetailsByTaskId(int taskId, int parentLimit, int childLimit) throws ServiceException {
        TaskDetails taskDetails = new TaskDetails();
        try {
            ManagementStatsDAO msDAO = new ManagementStatsDAO();
            return DAOTransactionContext.doInTransaction(tx -> {
                TopTaskInfo topTaskInfo = msDAO.getTopTaskByTaskId(tx, taskId);
                taskDetails.setTopTaskInfo(topTaskInfo);
                taskDetails.getParentTaskInfos().addAll(msDAO.getParentTasksByTopTaskId(tx, topTaskInfo.getId(), parentLimit));
                taskDetails.getChildTaskInfos().addAll(msDAO.getChildTasksByTopTaskId(tx, topTaskInfo.getId(), childLimit));
                return taskDetails;
            });
        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }
    }

    public TaskDetails getTaskDetailsByUuid(UUID uuid, int parentLimit, int childLimit) throws ServiceException {
        TaskDetails taskDetails = new TaskDetails();
        if (uuid == null) {
            // TODO: do real exception stuff here
            throw new RuntimeException("Error: no uuid was provided");
        } else {
            try {
                ManagementStatsDAO msDAO = new ManagementStatsDAO();
                return DAOTransactionContext.doInTransaction(tx -> {
                    TopTaskInfo topTaskInfo = msDAO.getTopTaskByUuid(tx, uuid);
                    taskDetails.setTopTaskInfo(topTaskInfo);
                    taskDetails.getParentTaskInfos().addAll(msDAO.getParentTasksByTopTaskId(tx, topTaskInfo.getId(), parentLimit));
                    taskDetails.getChildTaskInfos().addAll(msDAO.getChildTasksByTopTaskId(tx, topTaskInfo.getId(), childLimit));
                    return taskDetails;
                });
            } catch (Exception ex) {
                // TODO: do real exception stuff here
                throw new ServiceException("Error", ex);
            }
        }
    }

    public List<ConnectionDetails> getConnectionInfo() throws ServiceException {
        try {
            ManagementStatsDAO msDAO = new ManagementStatsDAO();
            return DAOTransactionContext.doInTransaction(tx -> {
                return msDAO.getSshStats(tx);
            });
        } catch (Exception ex) {
            // TODO: do real exception stuff here
            throw new ServiceException("Error", ex);
        }
    }

    /* ******************* Transfer Worker Stuff ****************** */
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

    /* ******************* Parent Task Stuff ****************** */
    private List<AssignedParentTaskInfo> getAssignedParentTasks(Set<TransferWorkerInfo> transferWorkers) throws SchedulingPolicyException {
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
    private List<ParentTaskInfo> getParentInProgressTasks() throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getParentTasksInStatus(tx, List.of(TransferTaskStatus.IN_PROGRESS));
        });
    }

    /* ******************* Child Task Stuff ****************** */
    private List<AssignedChildTaskInfo> getAssignedChildTasks(Set<TransferWorkerInfo> transferWorkers) throws SchedulingPolicyException {
        return transferWorkers.stream().flatMap(transferWorker -> {
            try {
                return getAssignedChildTasksForWorker(transferWorker).stream();
            } catch (SchedulingPolicyException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }
    private List<AssignedChildTaskInfo> getAssignedChildTasksForWorker(TransferWorkerInfo transferWorker) throws SchedulingPolicyException {
        List<AssignedChildTaskInfo> assignedChildTaskInfos = schedulingPolicy.getChildTasksForWorker(
                UUID.fromString(transferWorker.getUuid())).stream().map(prioritizedChildTask -> {
            TransferTaskChild ttp = prioritizedChildTask.getObject();
            return AssignedChildTaskInfo.fromChildTask(prioritizedChildTask.getPriority(), ttp);
        }).toList();
        return assignedChildTaskInfos;
    }

    private List<ChildTaskInfo> getQueuedChildTasks(List<Integer> queuedChildTaskIds) throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getChildTasks(tx, queuedChildTaskIds);
        });
    }

    private List<ChildTaskInfo> getChildTasksWaitingForRetry() throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getChildTasksInStatus(tx, List.of(TransferTaskStatus.AWAITING_RETRY));
        });
    }
    private List<ChildTaskInfo> getChildInProgressTasks() throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getChildTasksInStatus(tx, List.of(TransferTaskStatus.IN_PROGRESS));
        });
    }

    /* ******************* Top Task Stuff ****************** */
    private List<TopTaskInfo> getTopInProgressTasks() throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getTopTasksInStatus(tx, List.of(TransferTaskStatus.IN_PROGRESS), Optional.empty());
        });
    }

    private List<TopTaskInfo> getTopTaskRecentSuccesses(int limit) throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getTopTasksInStatus(tx, List.of(TransferTaskStatus.COMPLETED),  Optional.of(limit));
        });
    }
    private List<TopTaskInfo> getTopTaskRecentErrors(int limit) throws DAOException {
        ManagementStatsDAO msDao = new ManagementStatsDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            return msDao.getTopTasksInStatus(tx, List.of(TransferTaskStatus.FAILED), Optional.of(limit));
        });
    }

}
