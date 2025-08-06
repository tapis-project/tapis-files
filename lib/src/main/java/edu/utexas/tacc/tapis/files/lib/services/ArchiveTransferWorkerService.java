package edu.utexas.tacc.tapis.files.lib.services;

import com.google.common.base.Stopwatch;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.transfers.DefaultSchedulingPolicy;
import edu.utexas.tacc.tapis.files.lib.transfers.SchedulingPolicy;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ArchiveTransferWorkerService {
    private ScheduledExecutorService archiveTransferScheduler = Executors.newSingleThreadScheduledExecutor();
    private static final int MAX_THREADS = RuntimeSettings.get().getChildThreadPoolSize();
    private static final Logger log = LoggerFactory.getLogger(ArchiveTransferWorkerService.class);

    // this parameter is slightly confusing.  For each combination of tenant/user we will get a maximum of
    // this many items.  For example if there are 3 users (2 in one tenant and 1 in another), and the each have
    // exactly 3 tasks, and MAX_WORK_ITEM_DEPTH is set to 2 we will get back a max of 2 per user, so 6 items.  If
    // one of those users only had 1 task, we would get 2 for the first 2 users, and one for that user.  Hopefully
    // this makes sense - if not please update the comment :)
    private static final int MAX_WORK_ITEM_DEPTH = 100;
    private ExecutorService archiveTransferWorkers = Executors.newFixedThreadPool(MAX_THREADS, new ThreadFactory() {
        ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        @Override
        public Thread newThread(@NotNull Runnable runnable) {
            Thread th = defaultFactory.newThread(runnable);
            th.setDaemon(true);
            return th;
        }
    });



    public void start(UUID myUuid) {
        Map<UUID, Future<TransferTaskChild>> futures = new ConcurrentHashMap<UUID, Future<TransferTaskChild>>();

        // max number of futures to store in the futures map
        int maxFutures = MAX_THREADS * 5;

        archiveTransferScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean shouldExit = false;

                    SchedulingPolicy schedulingPolicy = new DefaultSchedulingPolicy(MAX_WORK_ITEM_DEPTH);

                    while (!shouldExit) {
                        if(!canCreateNewFutures(futures, maxFutures)) {
                            Thread.yield();
                            continue;
                        }
                        try {
                            List<PrioritizedObject<ArchiveTransfer>> atList = schedulingPolicy.getArchiveTransfersForWorker(myUuid);
                            for (PrioritizedObject<ArchiveTransfer> transfer : atList) {
                                System.out.println("transfer");
                            }
                        } catch (SchedulingPolicyException ex) {
                            log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERROR_GETTING_WORK", myUuid));
                            break;
                        }
                            /*
                        try {
                            List<PrioritizedObject<TransferTaskChild>> ttcList = schedulingPolicy.getChildTasksForWorker(myUuid);
                            for (PrioritizedObject<TransferTaskChild> ttc : ttcList) {
                                UUID childUuid = ttc.getObject().getUuid();
                                if (futures.containsKey(childUuid)) {
                                    if (futures.get(childUuid).isDone()) {
                                        futures.remove(childUuid);
                                    }
                                } else {
                                    log.debug("Priority: " + ttc.getPriority() + " tenant: " + ttc.getObject().getTenantId() + " user:" + ttc.getObject().getUsername());
                                    try {
                                        Future<TransferTaskChild> future = archiveTransferWorkers.submit(new Callable<TransferTaskChild>() {
                                            @Override
                                            public TransferTaskChild call() throws Exception {
                                                Stopwatch sw = Stopwatch.createStarted();
                                                try {
                                                    return handleTask(ttc.getObject());
                                                } catch (Throwable th) {
                                                    log.error("Caught exception while handling transfer task", th);
                                                }
                                                log.trace("CHILD TRANSFER TIMING: TransferTaskChild callable childId: " + ttc.getObject().getId() + " time: " + sw.elapsed(TimeUnit.MILLISECONDS));
                                                return null;
                                            }
                                        });
                                        futures.put(childUuid, future);
                                    } catch (Throwable th) {
                                        TransferTaskChild childTask = dao.getChildTaskByUUID(childUuid);
                                        childTask.setStatus(childTask.isOptional() ? TransferTaskStatus.FAILED_OPT : TransferTaskStatus.FAILED);
                                        dao.updateTransferTaskChild(childTask);
                                    }
                                }
                            }
                        } catch (DAOException | SchedulingPolicyException ex) {
                            log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERROR_GETTING_WORK", myUuid));
                            break;
                        }
                             */

                        if (futures.isEmpty()) {
                            shouldExit = true;
                        }
                    }
                } catch (Throwable th) {
                    // if this method throws, it will not get rescheduled.  We would have a zombie worker.  I think the
                    // best thing to do here is exit - we have caught some completely unexpected exception
                    System.exit(0);
                }
                Thread.yield();
            }
        }, 5, 5, TimeUnit.SECONDS);

    }

    private boolean canCreateNewFutures(Map<UUID, Future<TransferTaskChild>> futures, int capacity) {
        if(futures.size() >= capacity) {
            for (UUID key : futures.keySet()) {
                if (futures.get(key).isDone()) {
                    futures.remove(key);
                }
            }
        }

        return futures.size() < capacity;
    }

}
