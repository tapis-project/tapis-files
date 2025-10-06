package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.PostgresDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskChildDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskParentDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferWorkerDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferWorkerConfig;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/*
 * This class is the "main" for the transfer assigner.  The transfer assigner checks the transfer_workers
 * table to see which workers to assign work to.  It also checks for workers to go away or stop resoponding
 * (i.e. stop updating the trasnfer_workers timestamp).  If aw worker goes away any takss assigned to that
 * worker will get reassigned.
 */
public class TransfersAssigner
{
    private static final Logger log = LoggerFactory.getLogger(TransfersApp.class);
    private static int WORKER_BACKLOG_THRESHOLD = 100;
    private static int ROW_NUMBER_CUTOFF = 300;
    private static long EXPECT_HEARTBEAT_BEFORE_MILLIS = 180000;
    private static int MAX_WAIT_MULTIPLIER = RuntimeSettings.get().getMaxAssignmentWaitMultiplier();
    private SchedulingPolicy schedulingPolicy = new DefaultSchedulingPolicy(ROW_NUMBER_CUTOFF);

    public static void main(String[] args)
    {
        log.info("Starting transfers dispatcher.");
        try {
            checkRequiredSettings();
            TransfersAssigner assigner = new TransfersAssigner();
            assigner.run();
        } catch(Exception ex) {
            String msg = LibUtils.getMsg("FILES_TRANSFER_SCHEDULER_APPLICATION_FAILED_TO_START", ex.getMessage());
            log.error(msg, ex);
        }
    }

    // main loop for the transfers dispatcher.  Assign children, assign parents, do cleanup.  If we didn't assignthing
    // this time through the loop, sleep a little before going around again.
    void run() throws InterruptedException {
        boolean moreParentsToSchedule, moreChildrenToSchedule, moreArchiveTransfersToSchedule = false;
        int loopsWithNoWork = 0;
        for(;;) {
            try {
                // Assign child and parent tasks.  Keep track of if there's more work of each
                // to do.
                moreChildrenToSchedule = assignChildTasks();

                // Assign child and parent tasks.  Keep track of if there's more work of each
                // to do.
                moreParentsToSchedule = assignParentTasks();

                // Assign archive transfers.  Keep track of if there's more work of each
                // to do.
                moreArchiveTransfersToSchedule = assignArchiveTransfers();

                // get rid of any tasks that have a worker that no longer exists assigned to it.
                cleanupZombieAssignments();

                // sleep progressively longer (up to a max) if we don't have more work to do, but don't sleep
                // if there's work to do.
                if (!moreChildrenToSchedule && !moreParentsToSchedule && !moreArchiveTransfersToSchedule) {
                    if(loopsWithNoWork < MAX_WAIT_MULTIPLIER) {
                        loopsWithNoWork++;
                    }
                    int sleepTime = 500 << loopsWithNoWork;
                    log.trace("Sleeping for " + sleepTime + " milliseconds");
                    Thread.sleep(sleepTime);
                } else {
                    loopsWithNoWork = 0;
                }
            } catch (SchedulingPolicyException ex) {
                log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "scheduleWork", ex));
            }
        }
    }

    private static void checkRequiredSettings() {
        PostgresDAO pgDao = new PostgresDAO();
        try {
            DAOTransactionContext.doInTransaction(context -> {
                long postgresVersion = pgDao.getPostgresVersion(context);
                if (postgresVersion < RuntimeSettings.get().getRequiredPostgresVersion()) {
                    throw new RuntimeException(LibUtils.getMsg("FILES_TXFR_UNSUPPORTED_POSTGRES_VERSION", postgresVersion, RuntimeSettings.get().getRequiredPostgresVersion()));
                }
                return postgresVersion;
            });
        } catch (DAOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        StringBuilder missingVars = new StringBuilder();
        if (RuntimeSettings.get().getDbUrl() == null) {
            missingVars.append("DB_URL ");
        }

        if (RuntimeSettings.get().getDbUsername() == null) {
            missingVars.append("DB_USERNAME ");
        }

        if (RuntimeSettings.get().getDbPassword() == null) {
            missingVars.append("DB_PASSWORD ");
        }

        if(!missingVars.isEmpty()) {
            throw new RuntimeException(LibUtils.getMsg("FILES_TRANSFER_SCHEDULER_SERVICE_MISSING_REQUIRED_VARIABLES", missingVars.toString()));
        }

        try {
            DAOTransactionContext.doInTransaction(context -> {
                long postgresVersion = pgDao.getPostgresVersion(context);
                if (postgresVersion < RuntimeSettings.get().getRequiredPostgresVersion()) {
                    throw new RuntimeException(LibUtils.getMsg("FILES_TXFR_UNSUPPORTED_POSTGRES_VERSION", postgresVersion, RuntimeSettings.get().getRequiredPostgresVersion()));
                }
                return postgresVersion;
            });
        } catch (DAOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    void updateWorkerList(Map<UUID, Integer> activeWorkerMap, TransferWorkerConfig.TransferType forTransferType) {
        TransferWorkerDAO transferWorkerDAO = new TransferWorkerDAO();

        List<TransferWorker> workers = null;
        try {
            workers = DAOTransactionContext.doInTransaction((context) ->
                    transferWorkerDAO.getTransferWorkers(context).stream()
                            // only workers that will accept this type of transfer
                            .filter(transferWorker -> transferWorker.canAssignTask(new TransferWorker.AssignmentParams(forTransferType, null)))
                            .collect(Collectors.toList())
            );
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "updateWorkerList", ex));
        }

        for(TransferWorker worker : workers) {
            if(Instant.now().minusMillis(EXPECT_HEARTBEAT_BEFORE_MILLIS).isBefore(worker.getLastUpdated())) {
                // the worker is not an expired worker, so add or update it in the active work map.  We will
                // set the count to 0, but it will get updated later.
                activeWorkerMap.put(worker.getUuid(), 0);
            } else {
                // the work is expired, so remove it from the map, and the database
                if(activeWorkerMap.containsKey(worker.getUuid())) {
                    activeWorkerMap.remove(worker.getUuid());
                }
                cleanupZombieWorker(worker.getUuid());
            }
        }
    }

    private void cleanupZombieWorker(UUID workerUuid) {
        TransferWorkerDAO transferWorkerDAO = new TransferWorkerDAO();
        try {
            DAOTransactionContext.doInTransaction(context -> {
                transferWorkerDAO.deleteTransferWorkerById(context, workerUuid);
                return null;
            });
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "cleanupDeadWorker", ex));
        }
    }

    private void cleanupZombieAssignments() {
        TransferTaskChildDAO childTaskDao = new TransferTaskChildDAO();
        TransferTaskParentDAO parentTaskDao = new TransferTaskParentDAO();
        ArchiveTransfersDAO archiveTransfersDAO = new ArchiveTransfersDAO();
        try {
            DAOTransactionContext.doInTransaction(context -> {
                childTaskDao.cleanupZombieChildAssignments(context, TransferTaskChild.TERMINAL_STATES);
                parentTaskDao.cleanupZombieParentAssignments(context, TransferTaskParent.TERMINAL_STATES);
                archiveTransfersDAO.cleanupZombieArchiveTransferAssignments(context);
                return 0;
            });
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "cleanupZombieAssignments", ex));
        }

    }

    // Get the count of child tasks assigned to each worker uuid in the activeWorkerMap
    private void updateChildWorkCounts(Map<UUID, Integer> activeWorkerMap) {
        TransferTaskChildDAO transferTaskChildDAO = new TransferTaskChildDAO();

        try {
            Map<UUID, Integer> assignedWorkerCount = DAOTransactionContext.doInTransaction((context -> {
                return transferTaskChildDAO.getAssignedWorkerCount(context);
            }));

            for(var workerUuid : assignedWorkerCount.keySet()) {
                if(activeWorkerMap.containsKey(workerUuid)) {
                    // update the count in the active worker map from the query we just did
                    activeWorkerMap.put(workerUuid, assignedWorkerCount.get(workerUuid));
                }
            }
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "updateWorkCounts", ex));
        }
    }


    // Get the count of parent tasks assigned to each worker uuid in the activeWorkerMap
    private void updateParentWorkCounts(Map<UUID, Integer> activeWorkerMap) {
        TransferTaskParentDAO parentDao = new TransferTaskParentDAO();

        try {
            Map<UUID, Integer> assignedWorkerCount = DAOTransactionContext.doInTransaction((context -> {
                return parentDao.getAssignedWorkerCount(context);
            }));

            for(var workerUuid : assignedWorkerCount.keySet()) {
                if(activeWorkerMap.containsKey(workerUuid)) {
                    // update the count in the active worker map from the query we just did
                    activeWorkerMap.put(workerUuid, assignedWorkerCount.get(workerUuid));
                }
            }
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "updateWorkCounts", ex));
        }
    }

    // Get the count of parent tasks assigned to each worker uuid in the activeWorkerMap
    private void updateArchiveTransferWorkCounts(Map<UUID, Integer> activeWorkerMap) {
        ArchiveTransfersDAO archiveTransfersDAO = new ArchiveTransfersDAO();

        try {
            Map<UUID, Integer> assignedWorkerCount = DAOTransactionContext.doInTransaction((context -> {
                return archiveTransfersDAO.getAssignedWorkerCount(context);
            }));

            for(var workerUuid : assignedWorkerCount.keySet()) {
                if(activeWorkerMap.containsKey(workerUuid)) {
                    // update the count in the active worker map from the query we just did
                    activeWorkerMap.put(workerUuid, assignedWorkerCount.get(workerUuid));
                }
            }
        } catch (DAOException ex) {
            log.error(LibUtils.getMsg("FILES_TXFR_SCHEDULER_ERROR", "updateWorkCounts", ex));
        }
    }

    // return workers that "need work".  This is determined by building a map with key of worker uuid,
    // and value equal to the number of child tasks assigned to that worker uuid.  Then go through
    // each key and compare the count to "WORKER_BACKLOG_THRESHOLD".  If it's less, add the worker to
    // the workers that need work list.  Return the list.
    private List<UUID> getWorkersThatNeedChildTasks() {
        Map<UUID, Integer> activeWorkerMap = new HashMap<>();

        updateWorkerList(activeWorkerMap, TransferWorkerConfig.TransferType.TRANSFER_TYPE_CHILD);
        updateChildWorkCounts(activeWorkerMap);

        List<UUID> workersThatNeedWork = new ArrayList<>();

        // figure out which workers need work
        for(var workerUuid : activeWorkerMap.keySet()) {
            int count = activeWorkerMap.get(workerUuid).intValue();
            if(count < WORKER_BACKLOG_THRESHOLD) {
                workersThatNeedWork.add(workerUuid);
            }
        }

        return workersThatNeedWork;
    }

    //  Assigns child tasks and returns true if there are more that need to be assigned, or false if
    //  not.
    private boolean assignChildTasks() throws SchedulingPolicyException {
        // find the uuid's of the workers that need more child tasks.
        List<UUID> workersThatNeedWork = getWorkersThatNeedChildTasks();

        // get all of the task ids that need to be assigned
        List<Integer> queuedTaskIds = schedulingPolicy.getQueuedChildTaskIds();

        // do the actual assignment of tasks to workers
        schedulingPolicy.assignChildTasksToWorkers(workersThatNeedWork, queuedTaskIds);

        // TODO:  I can't remember if/why I need this.  Put comment here if you figure it out!  Harmless, but possibly unneeded.
        // if there are still workers that need work and there are still tasks left in the list
        // continue to do assignments.
        // workersThatNeedWork = getWorkersThatNeedChildTasks();
        queuedTaskIds = schedulingPolicy.getQueuedChildTaskIds();
        return (!queuedTaskIds.isEmpty());
    }

    // return workers that "need work".  This is determined by building a map with key of worker uuid,
    // and value equal to the number of parent tasks assigned to that worker uuid.  Then go through
    // each key and compare the count to "WORKER_BACKLOG_THRESHOLD".  If it's less, add the worker to
    // the workers that need work list.  Return the list.
    private List<UUID> getWorkersThatNeedParentTasks() {
        Map<UUID, Integer> activeWorkerMap = new HashMap<>();

        updateWorkerList(activeWorkerMap, TransferWorkerConfig.TransferType.TRANSFER_TYPE_PARENT);
        updateParentWorkCounts(activeWorkerMap);

        List<UUID> workersThatNeedWork = new ArrayList<>();

        // figure out which workers need work
        for(var workerUuid : activeWorkerMap.keySet()) {
            int count = activeWorkerMap.get(workerUuid).intValue();
            if(count < WORKER_BACKLOG_THRESHOLD) {
                workersThatNeedWork.add(workerUuid);
            }
        }

        return workersThatNeedWork;
    }

    private boolean assignParentTasks() throws SchedulingPolicyException {
        // find the uuid's of the workers that need more parent tasks.
        List<UUID> workersThatNeedWork = getWorkersThatNeedParentTasks();

        // get all of the task ids that need to be assigned
        List<Integer> queuedTaskIds = schedulingPolicy.getQueuedParentTaskIds();

        // do the actual assignment of tasks to workers
        schedulingPolicy.assignParentTasksToWorkers(workersThatNeedWork, queuedTaskIds);

        // TODO:  I can't remember if/why I need this.  Put comment here if you figure it out!  Harmless, but possibly unneeded.
        // if there are still workers that need work and there are still tasks left in the list
        // continue to do assignments.
        // workersThatNeedWork = getWorkersThatNeedParentTasks();
        queuedTaskIds = schedulingPolicy.getQueuedParentTaskIds();
        return (!queuedTaskIds.isEmpty());
    }

    // return workers that "need work".  This is determined by building a map with key of worker uuid,
    // and value equal to the number of archive transfers assigned to that worker uuid.  Then go through
    // each key and compare the count to "WORKER_BACKLOG_THRESHOLD".  If it's less, add the worker to
    // the workers that need work list.  Return the list.
    private List<UUID> getWorkersThatNeedArchiveTransfers() {
        Map<UUID, Integer> activeWorkerMap = new HashMap<>();

        updateWorkerList(activeWorkerMap, TransferWorkerConfig.TransferType.TRANSFER_TYPE_ARCHIVE);
        updateArchiveTransferWorkCounts(activeWorkerMap);

        List<UUID> workersThatNeedWork = new ArrayList<>();

        // figure out which workers need work
        for(var workerUuid : activeWorkerMap.keySet()) {
            int count = activeWorkerMap.get(workerUuid).intValue();
            if(count < WORKER_BACKLOG_THRESHOLD) {
                workersThatNeedWork.add(workerUuid);
            }
        }

        return workersThatNeedWork;
    }

    private boolean assignArchiveTransfers() throws SchedulingPolicyException {
        // find the uuid's of the workers that need more parent tasks.
        List<UUID> workersThatNeedWork = getWorkersThatNeedArchiveTransfers();

        // get all of the task ids that need to be assigned
        List<Integer> queuedTaskIds = schedulingPolicy.getQueuedArchiveTransferIds();

        // do the actual assignment of tasks to workers
        schedulingPolicy.assignArchiveTransfersToWorkers(workersThatNeedWork, queuedTaskIds);

        // TODO:  I can't remember if/why I need this.  Put comment here if you figure it out!  Harmless, but possibly unneeded.
        // if there are still workers that need work and there are still tasks left in the list
        // continue to do assignments.
        // workersThatNeedWork = getWorkersThatNeedArchiveTransfers();
        queuedTaskIds = schedulingPolicy.getQueuedArchiveTransferIds();
        return (!queuedTaskIds.isEmpty());
    }
}
