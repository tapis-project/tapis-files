package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.dao.ChildTaskQuery;
import edu.utexas.tacc.tapis.files.lib.dao.QueryBuilder;
import edu.utexas.tacc.tapis.files.lib.dao.ParentTaskQuery;
import edu.utexas.tacc.tapis.files.lib.dao.QueryCondition;
import edu.utexas.tacc.tapis.files.lib.dao.TopTaskQuery;
import edu.utexas.tacc.tapis.files.lib.dao.stats.SSHSessionLogDAO;
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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class AppStatusService {
    public static final int CACHED_ROWS = 300;
    SchedulingPolicy schedulingPolicy = new DefaultSchedulingPolicy(CACHED_ROWS);
    // TODO:  rename unnassigned to queued_unassigned or something like that.  Make other names match too.
    public StatusSummary getStatusSummary() throws ServiceException {

        try {
            StatusSummary statusSummary = new StatusSummary();
            FileTransfersDAO topTaskDAO = new FileTransfersDAO();
            TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
            TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
            return DAOTransactionContext.doInTransaction(tx -> {
                // top task counts
                {
                    TopTaskQuery query = new TopTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.ACCEPTED.name())
                    );
                    statusSummary.setQueuedTopTasks(topTaskDAO.countTopTasks(tx, query));
                }
                {
                    TopTaskQuery query = new TopTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.IN_PROGRESS.name())
                    );
                    statusSummary.setInProgressTopTasks(topTaskDAO.countTopTasks(tx, query));
                }

                // parent task counts
                {
                    ParentTaskQuery query = new ParentTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.IN_PROGRESS.name())
                    );
                    statusSummary.setInProgressParentTasks(parentTaskDAO.countParentTasks(tx, query));
                }
                {
                    ParentTaskQuery query = new ParentTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isNotNull(ParentTaskQuery.COMPARE_FIELD_ASSIGNED_TO)
                    );
                    statusSummary.setAssignedParentTasks(parentTaskDAO.countParentTasks(tx, query));
                }
                {
                    ParentTaskQuery query = new ParentTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.and(
                            QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.ACCEPTED.name()),
                            QueryCondition.isNull(ParentTaskQuery.COMPARE_FIELD_ASSIGNED_TO)
                        )
                    );
                    statusSummary.setUnassignedParentTasks(parentTaskDAO.countParentTasks(tx, query));
                }
                {
                    ParentTaskQuery query = new ParentTaskQuery();
                    query.setQueryCondition(
                            QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.AWAITING_RETRY.name())
                    );
                    statusSummary.setAwaitingRetryParentTasks(parentTaskDAO.countParentTasks(tx, query));
                }

                // child task counts
                {
                    ChildTaskQuery query = new ChildTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.IN_PROGRESS.name())
                    );
                    statusSummary.setInProgressChildTasks(childTaskDAO.countChildTasks(tx, query));
                }
                {
                    ChildTaskQuery query = new ChildTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isNotNull(ChildTaskQuery.COMPARE_FIELD_ASSIGNED_TO)
                    );
                    statusSummary.setAssignedChildTasks(childTaskDAO.countChildTasks(tx, query));
                }
                {
                    ChildTaskQuery query = new ChildTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.and(
                            QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.ACCEPTED.name()),
                            QueryCondition.isNull(ChildTaskQuery.COMPARE_FIELD_ASSIGNED_TO)
                        )
                    );
                    statusSummary.setUnassignedChildTasks(childTaskDAO.countChildTasks(tx, query));
                }
                {
                    ChildTaskQuery query = new ChildTaskQuery();
                    query.setQueryCondition(
                        QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.AWAITING_RETRY.name())
                    );
                    statusSummary.setAwaitingRetryChildTasks(childTaskDAO.countChildTasks(tx, query));
                }
                return statusSummary;
            });
        } catch (Exception ex) {
            throw new ServiceException("Error retrieving status summary", ex);
        }
    }

    public TopTaskDetails getTopTaskDetails(int successLimit, int errorLimit) throws ServiceException {
        TopTaskDetails topTaskDetails = new TopTaskDetails();
        try {
            topTaskDetails.getTopInProgress().addAll(getTopInProgressTasks());
            topTaskDetails.getRecentSuccesses().addAll(getTopTaskRecentSuccesses(successLimit));
            topTaskDetails.getRecentErrors().addAll(getTopTaskRecentErrors(errorLimit));
        } catch (Exception ex) {
            throw new ServiceException("Error retrieving top task details", ex);
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
            throw new ServiceException("Error retrieving parent task details", ex);
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
            throw new ServiceException("Error retrieving child task details", ex);
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
                topTaskQuery.setQueryCondition(
                        QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_ID, taskId)
                );
                topTaskQuery.addSortField(TopTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
                TopTaskInfo topTaskInfo = topTaskDAO.getTopTaskInfo(tx, topTaskQuery);
                taskDetails.setTopTaskInfo(topTaskInfo);

                ParentTaskQuery parentTaskQuery = new ParentTaskQuery();
                parentTaskQuery.setQueryCondition(
                        QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_TOP_TASK_ID, topTaskInfo.getId())
                );
                parentTaskQuery.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
                parentTaskQuery.setLimit(parentLimit);
                taskDetails.getParentTaskInfos().addAll(parentTaskDAO.getParentTaskInfos(tx, parentTaskQuery));

                ChildTaskQuery childTaskQuery = new ChildTaskQuery();
                childTaskQuery.setQueryCondition(
                        QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_TOP_TASK_ID, topTaskInfo.getId())
                );
                childTaskQuery.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
                childTaskQuery.setLimit(childLimit);
                taskDetails.getChildTaskInfos().addAll(childTaskDAO.getChildTaskInfos(tx, childTaskQuery));
                return taskDetails;
            });
        } catch (Exception ex) {
            throw new ServiceException("Error getting task details by id for id: " + taskId, ex);
        }
    }

    public TaskDetails getTaskDetailsByUuid(UUID uuid, int parentLimit, int childLimit) throws ServiceException {
        TaskDetails taskDetails = new TaskDetails();
        if (uuid == null) {
            throw new ServiceException("Error: no uuid was provided");
        } else {
            try {
                FileTransfersDAO topTaskDAO = new FileTransfersDAO();
                TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
                TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
                return DAOTransactionContext.doInTransaction(tx -> {
                    TopTaskQuery topTaskQuery = new TopTaskQuery();
                    topTaskQuery.setQueryCondition(
                            QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_UUID, uuid)
                    );
                    topTaskQuery.addSortField(TopTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
                    TopTaskInfo topTaskInfo = topTaskDAO.getTopTaskInfo(tx, topTaskQuery);
                    taskDetails.setTopTaskInfo(topTaskInfo);

                    ParentTaskQuery parentTaskQuery = new ParentTaskQuery();
                    parentTaskQuery.setQueryCondition(
                            QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_TOP_TASK_ID, topTaskInfo.getId())
                    );
                    parentTaskQuery.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
                    parentTaskQuery.setLimit(parentLimit);
                    taskDetails.getParentTaskInfos().addAll(parentTaskDAO.getParentTaskInfos(tx, parentTaskQuery));

                    ChildTaskQuery childTaskQuery = new ChildTaskQuery();
                    childTaskQuery.setQueryCondition(
                            QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_TOP_TASK_ID, topTaskInfo.getId())
                    );
                    childTaskQuery.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
                    childTaskQuery.setLimit(childLimit);
                    taskDetails.getChildTaskInfos().addAll(childTaskDAO.getChildTaskInfos(tx, childTaskQuery));
                    return taskDetails;
                });
            } catch (Exception ex) {
                throw new ServiceException("Error getting task details by uuid for uuid: " + uuid, ex);
            }
        }
    }

    public List<ConnectionDetails> getConnectionInfo() throws ServiceException {
        try {
            SSHSessionLogDAO msDAO = new SSHSessionLogDAO();
            return DAOTransactionContext.doInTransaction(tx -> {
                return msDAO.getSshStats(tx);
            });
        } catch (Exception ex) {
            throw new ServiceException("Error getting connection information", ex);
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
            query.setQueryCondition(
                    QueryCondition.isIn(ParentTaskQuery.COMPARE_FIELD_ID, queuedParentTaskIds)
            );
            query.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
            return parentTaskDAO.getParentTaskInfos(tx, query);
        });
    }

    private List<ParentTaskInfo> getParentTasksWaitingForRetry() throws DAOException {
        TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ParentTaskQuery query = new ParentTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.AWAITING_RETRY.name())
            );
            query.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
            return parentTaskDAO.getParentTaskInfos(tx, query);
        });
    }
    private List<ParentTaskInfo> getParentInProgressTasks() throws DAOException {
        TransferTaskParentDAO parentTaskDAO = new TransferTaskParentDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ParentTaskQuery query = new ParentTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(ParentTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.IN_PROGRESS.name())
            );
            query.addSortField(ParentTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
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
            query.setQueryCondition(
                    QueryCondition.isIn(ChildTaskQuery.COMPARE_FIELD_ID, queuedChildTaskIds)
            );
            query.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
            return childTaskDAO.getChildTaskInfos(tx, query);
        });
    }

    private List<ChildTaskInfo> getChildTasksWaitingForRetry() throws DAOException {
        TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ChildTaskQuery query = new ChildTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.AWAITING_RETRY.name())
            );
            query.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
            return childTaskDAO.getChildTaskInfos(tx, query);
        });
    }
    private List<ChildTaskInfo> getChildInProgressTasks() throws DAOException {
        TransferTaskChildDAO childTaskDAO = new TransferTaskChildDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            ChildTaskQuery query = new ChildTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(ChildTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.IN_PROGRESS.name())
            );
            query.addSortField(ChildTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
            return childTaskDAO.getChildTaskInfos(tx, query);
        });
    }

    /* ******************* Top Task Stuff ****************** */
    private List<TopTaskInfo> getTopInProgressTasks() throws DAOException {
        FileTransfersDAO topTaskDAO = new FileTransfersDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            TopTaskQuery query = new TopTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.IN_PROGRESS.name())
            );
            query.addSortField(TopTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.ASC);
            return topTaskDAO.getTopTaskInfos(tx, query);
        });
    }

    private List<TopTaskInfo> getTopTaskRecentSuccesses(int limit) throws DAOException {
        FileTransfersDAO topTaskDAO = new FileTransfersDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            TopTaskQuery query = new TopTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.COMPLETED.name())
            );
            query.addSortField(TopTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.DESC);
            query.setLimit(limit);
            return topTaskDAO.getTopTaskInfos(tx, query);
        });
    }
    private List<TopTaskInfo> getTopTaskRecentErrors(int limit) throws DAOException {
        FileTransfersDAO topTaskDAO = new FileTransfersDAO();
        return DAOTransactionContext.doInTransaction(tx -> {
            TopTaskQuery query = new TopTaskQuery();
            query.setQueryCondition(
                    QueryCondition.isEqual(TopTaskQuery.COMPARE_FIELD_STATUS, TransferTaskStatus.FAILED.name())
            );
            query.addSortField(TopTaskQuery.SORT_FIELD_CREATED, QueryBuilder.SortOrder.DESC);
            query.setLimit(limit);
            return topTaskDAO.getTopTaskInfos(tx, query);
        });
    }


}
