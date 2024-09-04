package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskChildDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferTaskParentDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferWorkerDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/*
 * Class with main used to start a Files worker. See deploy/Dockerfile.workers
 *
 * When the TransferApp starts up, it calls startListenters() for the parent and child task
 * services.  This starts up listeners for the relavent queue (see the approprieat task
 * service class for more).  These listeners will handle each message that comes into the rabbitMQ queue
 * for the service (parent or child).
 *
 * Transfers processing:
 *
 * A request comes into the api for a transfer.  The request can contain multiple transfers.  When that
 * request is handled, the api creates a "top task" in the database.  The top task tracks the transfer
 * request.  For each actual transfer in the request, the api will create a "parent task" whcih gets
 * stored the the parent task table.  Finally the api will write a message into the rabbitMQ parent queue
 * for each parent created.
 *
 * When the parent task service reads a parent task message, it will process it by determining which
 * files are to be transferred for this request.  In the case of a file, there's just one, but in the
 * case of a directory it will walk the directory tree and note each file that must be transferred.
 * For each file that must be transferred a child task is created and written to the child task table
 * in the database.  For each of the child tasks a message pointing to the child task is written to the
 * child task queue.
 *
 * When the child task service reads a child task message from rabbitmq, it will process it by transferring
 * the file described by the child task message, and marking the task compoleted (or failed or whatever).
 */
public class TransfersDispatcher
{
    private static final Logger log = LoggerFactory.getLogger(TransfersApp.class);
    private static int WORKER_BACKLOG_THRESHOLD = 100;
    private static int ROW_NUMBER_CUTOFF = 300;
    private static long EXPECT_HEARTBEAT_BEFORE_MILLIS = 600000;
    private static long MAX_WAIT_MULTIPLIER = 5;
    private SchedulingPolicy schedulingPolicy = new DefaultSchedulingPolicy(ROW_NUMBER_CUTOFF);

    public static void main(String[] args)
    {
        log.info("Starting transfers dispatcher.");
        try {
            checkRequiredSettings();
            TransfersDispatcher dispatcher = new TransfersDispatcher();
            dispatcher.run();
        } catch(Exception ex) {
            String msg = LibUtils.getMsg("FILES_TRANSFER_SCHEDULER_APPLICATION_FAILED_TO_START", ex.getMessage());
            log.error(msg, ex);
        }
    }

    // main loop for the transfers dispatcher.  Assign children, assign parents, do cleanup.  If we didn't assignthing
    // this time through the loop, sleep a little before going around again.
    void run() throws InterruptedException {
        boolean moreParentsToSchedule, moreChildrenToSchedule = false;
        int loopsWithNoWork = 0;
        for(;;) {
            try {
                // Assign child and parent tasks.  Keep track of if there's more wore of each
                // to do.
                moreChildrenToSchedule = assignChildTasks();

                // Assign child and parent tasks.  Keep track of if there's more wore of each
                // to do.
                moreParentsToSchedule = assignParentTasks();

                // get rid of any tasks that have a worker that no longer exists assigned to it.
                cleanupZombieAssignments();

                // sleep progressively longer (up to a max) if we don't have more work to do, but don't sleep
                // if there's work to do.
                if (!moreChildrenToSchedule && !moreParentsToSchedule) {
                    if(loopsWithNoWork < MAX_WAIT_MULTIPLIER) {
                        loopsWithNoWork++;
                    }
                    int sleepTime = 500 << loopsWithNoWork;
                    log.warn("Sleeping for " + sleepTime + " milliseconds");
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
        StringBuilder missingVars = new StringBuilder();
        if (RuntimeSettings.get().getDbHost() == null) {
            missingVars.append("DB_HOST ");
        }

        if (RuntimeSettings.get().getDbName() == null) {
            missingVars.append("DB_NAME ");
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
    }

    void updateWorkerList(Map<UUID, Integer> activeWorkerMap) {
        TransferWorkerDAO transferWorkerDAO = new TransferWorkerDAO();

        List<TransferWorker> workers = null;
        try {
            workers = DAOTransactionContext.doInTransaction((context) -> {
                return transferWorkerDAO.getTransferWorkers(context);
            });
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
        try {
            DAOTransactionContext.doInTransaction(context -> {
                childTaskDao.cleanupZombieChildAssignments(context, TransferTaskChild.TERMINAL_STATES);
                parentTaskDao.cleanupZombieParentAssignments(context, TransferTaskParent.TERMINAL_STATES);
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

    // return workers that "need work".  This is determined by building a map with key of worker uuid,
    // and value equal to the number of child tasks assigned to that worker uuid.  Then go through
    // each key and compare the count to "WORKER_BACKLOG_THRESHOLD".  If it's less, add the worker to
    // the workers that need work list.  Return the list.
    private List<UUID> getWorkersThatNeedChildTasks() {
        Map<UUID, Integer> activeWorkerMap = new HashMap<>();

        updateWorkerList(activeWorkerMap);
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

        // if there are still workers that need work and there are still tasks left in the list
        // continue to do assignments.
        workersThatNeedWork = getWorkersThatNeedChildTasks();
        queuedTaskIds = schedulingPolicy.getQueuedChildTaskIds();
        return (!workersThatNeedWork.isEmpty() && !queuedTaskIds.isEmpty());
    }

    // return workers that "need work".  This is determined by building a map with key of worker uuid,
    // and value equal to the number of parent tasks assigned to that worker uuid.  Then go through
    // each key and compare the count to "WORKER_BACKLOG_THRESHOLD".  If it's less, add the worker to
    // the workers that need work list.  Return the list.
    private List<UUID> getWorkersThatNeedParentTasks() {
        Map<UUID, Integer> activeWorkerMap = new HashMap<>();

        updateWorkerList(activeWorkerMap);
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

        // if there are still workers that need work and there are still tasks left in the list
        // continue to do assignments.
        workersThatNeedWork = getWorkersThatNeedChildTasks();
        queuedTaskIds = schedulingPolicy.getQueuedParentTaskIds();
        return (!workersThatNeedWork.isEmpty() && !queuedTaskIds.isEmpty());
    }
}
