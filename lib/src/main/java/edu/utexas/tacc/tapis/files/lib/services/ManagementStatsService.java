package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.ChildTaskQuery;
import edu.utexas.tacc.tapis.files.lib.dao.FilesQueryBuilder;
import edu.utexas.tacc.tapis.files.lib.dao.ParentTaskQuery;
import edu.utexas.tacc.tapis.files.lib.dao.TopTaskQuery;
import edu.utexas.tacc.tapis.files.lib.dao.stats.ManagementStatsDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskChildDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskParentDAO;
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
import org.apache.commons.collections.CollectionUtils;
import org.jvnet.hk2.annotations.Service;

import java.util.Collections;
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
            FileTransfersDAO topTaskDAO = new FileTransfersDAO();
            TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
            TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();

            return DAOTransactionContext.doInTransaction(tx -> {
                TopTaskQuery topTaskQuery = new TopTaskQuery();
                topTaskQuery.addCondition(TopTaskQuery.COMPARE_FIELD_ID, FilesQueryBuilder.Comparator.EQUALS, taskId);
                topTaskQuery.addSortField(TopTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
                TopTaskInfo topTaskInfo = topTaskDAO.getTopTaskInfo(tx, topTaskQuery);
                taskDetails.setTopTaskInfo(topTaskInfo);

                ParentTaskQuery parentTaskQuery = new ParentTaskQuery();
                parentTaskQuery.addCondition(ParentTaskQuery.COMPARE_FIELD_TOP_TASK_ID, FilesQueryBuilder.Comparator.EQUALS, topTaskInfo.getId());
                parentTaskQuery.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
                parentTaskQuery.setLimit(parentLimit);
                taskDetails.getParentTaskInfos().addAll(parentTaskDAO.getParentTaskInfos(tx, parentTaskQuery));

                ChildTaskQuery childTaskQuery = new ChildTaskQuery();
                childTaskQuery.addCondition(ChildTaskQuery.COMPARE_FIELD_TOP_TASK_ID, FilesQueryBuilder.Comparator.EQUALS, topTaskInfo.getId());
                childTaskQuery.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
                childTaskQuery.setLimit(childLimit);
                taskDetails.getChildTaskInfos().addAll(childTaskDAO.getChildTaskInfos(tx, childTaskQuery));
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
                FileTransfersDAO topTaskDAO = new FileTransfersDAO();
                TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
                TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
                return DAOTransactionContext.doInTransaction(tx -> {
                    TopTaskQuery topTaskQuery = new TopTaskQuery();
                    topTaskQuery.addCondition(TopTaskQuery.COMPARE_FIELD_UUID, FilesQueryBuilder.Comparator.EQUALS, uuid);
                    topTaskQuery.addSortField(TopTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
                    TopTaskInfo topTaskInfo = topTaskDAO.getTopTaskInfo(tx, topTaskQuery);
                    taskDetails.setTopTaskInfo(topTaskInfo);

                    ParentTaskQuery parentTaskQuery = new ParentTaskQuery();
                    parentTaskQuery.addCondition(ParentTaskQuery.COMPARE_FIELD_TOP_TASK_ID, FilesQueryBuilder.Comparator.EQUALS, topTaskInfo.getId());
                    parentTaskQuery.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
                    parentTaskQuery.setLimit(parentLimit);

                    ChildTaskQuery childTaskQuery = new ChildTaskQuery();
                    childTaskQuery.addCondition(ChildTaskQuery.COMPARE_FIELD_TOP_TASK_ID, FilesQueryBuilder.Comparator.EQUALS, topTaskInfo.getId());
                    childTaskQuery.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
                    childTaskQuery.setLimit(childLimit);
                    taskDetails.getChildTaskInfos().addAll(childTaskDAO.getChildTaskInfos(tx, childTaskQuery));
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
        TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ParentTaskQuery query = new ParentTaskQuery();
            if(CollectionUtils.isEmpty(queuedParentTaskIds)) {
                return Collections.emptyList();
            }
            query.addCollectionConditon(ParentTaskQuery.COMPARE_FIELD_ID, FilesQueryBuilder.CollectionComparator.IN, queuedParentTaskIds);
            query.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return parentTaskDAO.getParentTaskInfos(tx, query);
        });
    }

    private List<ParentTaskInfo> getParentTasksWaitingForRetry() throws DAOException {
        TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ParentTaskQuery query = new ParentTaskQuery();
            query.addCondition(ParentTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS, TransferTaskStatus.AWAITING_RETRY.name());
            query.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return parentTaskDAO.getParentTaskInfos(tx, query);
        });
    }
    private List<ParentTaskInfo> getParentInProgressTasks() throws DAOException {
        TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ParentTaskQuery query = new ParentTaskQuery();
            query.addCondition(ParentTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS, TransferTaskStatus.IN_PROGRESS.name());
            query.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return parentTaskDAO.getParentTaskInfos(tx, query);
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
        TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ChildTaskQuery query = new ChildTaskQuery();

            // if there are no queued child task ids, no need to do a query.
            if(CollectionUtils.isEmpty(queuedChildTaskIds)) {
                return Collections.EMPTY_LIST;
            }
            query.addCollectionConditon(ChildTaskQuery.COMPARE_FIELD_ID, FilesQueryBuilder.CollectionComparator.IN,  queuedChildTaskIds);
            query.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return childTaskDAO.getChildTaskInfos(tx, query);
        });
    }

    private List<ChildTaskInfo> getChildTasksWaitingForRetry() throws DAOException {
        TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ChildTaskQuery query = new ChildTaskQuery();
            query.addCondition(ChildTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS,
                    TransferTaskStatus.AWAITING_RETRY.name());
            query.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return childTaskDAO.getChildTaskInfos(tx, query);
        });
    }
    private List<ChildTaskInfo> getChildInProgressTasks() throws DAOException {
        TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ChildTaskQuery query = new ChildTaskQuery();
            query.addCondition(ChildTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS,
                    TransferTaskStatus.AWAITING_RETRY.name());
            query.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return childTaskDAO.getChildTaskInfos(tx, query);
        });
    }

    /* ******************* Top Task Stuff ****************** */
    private List<TopTaskInfo> getTopInProgressTasks() throws DAOException {
        FileTransfersDAO topTaskDAO = new FileTransfersDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            TopTaskQuery query = new TopTaskQuery();
            query.addCondition(TopTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS,
                    TransferTaskStatus.IN_PROGRESS.name());
            query.addSortField(TopTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            return topTaskDAO.getTopTaskInfos(tx, query);
        });
    }

    private List<TopTaskInfo> getTopTaskRecentSuccesses(int limit) throws DAOException {
        FileTransfersDAO topTaskDAO = new FileTransfersDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            TopTaskQuery query = new TopTaskQuery();
            query.addCondition(TopTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS,
                    TransferTaskStatus.COMPLETED.name());
            query.addSortField(TopTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            query.setLimit(limit);
            return topTaskDAO.getTopTaskInfos(tx, query);
        });
    }
    private List<TopTaskInfo> getTopTaskRecentErrors(int limit) throws DAOException {
        FileTransfersDAO topTaskDAO = new FileTransfersDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            TopTaskQuery query = new TopTaskQuery();
            query.addCondition(TopTaskQuery.COMPARE_FIELD_STATUS, FilesQueryBuilder.Comparator.EQUALS,
                    TransferTaskStatus.FAILED.name());
            query.addSortField(TopTaskQuery.SORT_FIELD_CREATED, FilesQueryBuilder.SortOrder.ASC);
            query.setLimit(limit);
            return topTaskDAO.getTopTaskInfos(tx, query);
        });
    }

}
