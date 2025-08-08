package edu.utexas.tacc.tapis.files.lib.services;

import com.google.common.base.Stopwatch;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferDestination;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferResult;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferSource;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.TapisArchiveInputStream;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.PrioritizedObject;
import edu.utexas.tacc.tapis.files.lib.models.TransferURI;
import edu.utexas.tacc.tapis.files.lib.transfers.DefaultSchedulingPolicy;
import edu.utexas.tacc.tapis.files.lib.transfers.SchedulingPolicy;
import edu.utexas.tacc.tapis.files.lib.transfers.TransfersApp;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.sshd.common.io.WritePendingException;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory.IMPERSONATION_ID_NULL;

@Service
public class ArchiveTransferWorkerService {
    private ScheduledExecutorService archiveTransferScheduler = Executors.newSingleThreadScheduledExecutor();
    private static final int MAX_THREADS = RuntimeSettings.get().getArchiveTransferThreadPoolSize();
    private static final Logger log = LoggerFactory.getLogger(ArchiveTransferWorkerService.class);

    @Inject
    private RemoteDataClientFactory remoteDataClientFactory;
    @Inject
    private FileShareService shareService;
    @Inject
    private FilePermsService permsService;
    @Inject
    private SystemsCache systemsCache;
    @Inject
    private SystemsCacheNoAuth systemsCacheNoAuth;

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
        Map<UUID, Future<ArchiveTransfer>> futures = new ConcurrentHashMap<UUID, Future<ArchiveTransfer>>();

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
                            for (PrioritizedObject<ArchiveTransfer> prioritizedArchiveTransfer : atList) {
                                UUID archiveTransferUuid = prioritizedArchiveTransfer.getObject().getUuid();
                                if (futures.containsKey(archiveTransferUuid)) {
                                    if (futures.get(archiveTransferUuid).isDone()) {
                                        futures.remove(archiveTransferUuid);
                                    }
                                } else {
                                    log.debug("Priority: " + prioritizedArchiveTransfer.getPriority() + " tenant: " +
                                            prioritizedArchiveTransfer.getObject().getTenantId() +
                                            " user:" + prioritizedArchiveTransfer.getObject().getUsername());
                                    try {
                                        Future<ArchiveTransfer> future = archiveTransferWorkers.submit(new Callable<ArchiveTransfer>() {
                                            @Override
                                            public ArchiveTransfer call() throws Exception {
                                                Stopwatch sw = Stopwatch.createStarted();
                                                try {
                                                    return handleTask(prioritizedArchiveTransfer.getObject());
                                                } catch (Throwable th) {
                                                    log.error("Caught exception while handling transfer task", th);
                                                }
                                                log.trace("ARCHIVE TRANSFER TIMING: ArchiveTransfer callable Id: " + prioritizedArchiveTransfer.getObject().getId() + " time: " + sw.elapsed(TimeUnit.MILLISECONDS));
                                                return null;
                                            }
                                        });
                                        futures.put(archiveTransferUuid, future);
                                    } catch (Throwable th) {
                                        ArchiveTransfersDAO archiveTransfersDAO = new ArchiveTransfersDAO();
                                        // TODO AXFER: Re-enable this code
                                        /*
                                        ArchiveTransfer archiveTransfer = archiveTransfersDAO.getArchiveTransferByUUID(archiveTransferUuid);
                                        archiveTransfer.setStatus(archiveTransfer.isOptional() ? TransferTaskStatus.FAILED_OPT : TransferTaskStatus.FAILED);
                                        archiveTransfersDAO.updateTransferTaskChild(archiveTransfer);
                                         */
                                    }
                                }
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

    private boolean canCreateNewFutures(Map<UUID, Future<ArchiveTransfer>> futures, int capacity) {
        if(futures.size() >= capacity) {
            for (UUID key : futures.keySet()) {
                if (futures.get(key).isDone()) {
                    futures.remove(key);
                }
            }
        }

        return futures.size() < capacity;
    }

    private ArchiveTransfer handleTask(ArchiveTransfer archiveTransfer) throws IOException {
        //We are going to run the meat of the transfer, step2 in a separate Future which we can cancel.
        //This just sets up the future, we first subscribe to the control messages and then start the future
        //which is a blocking call.
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<ArchiveTransfer> future = executorService.submit(new Callable<ArchiveTransfer>() {
            @Override
            public ArchiveTransfer call() throws IOException, ServiceException {
                try {
                    return processTransfer(archiveTransfer);
                } catch (WritePendingException ex) {
                    throw new IOException(ex.getMessage(), ex);
                }
            }
        });

        //TODO AXFER: handle exception properly
        ArchiveTransfer completedTransfer = null;
        try {
            completedTransfer = future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return completedTransfer;
    }

    private ArchiveTransfer processTransfer(ArchiveTransfer archiveTransfer) throws IOException {
        final String opName = "processTransfer";

        ResourceRequestUser rUser = simulateResourceRequestUser(archiveTransfer);

        //TODO AXFER: need to check the protocol in the url, and system type and stuff like that.
        TransferURI srcUri = new TransferURI(archiveTransfer.getSourceBaseUrl());
        TapisSystem srcSystem = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache,
                systemsCacheNoAuth, permsService, opName, srcUri.getSystemId(), srcUri.getPath(),
                FileInfo.Permission.READ, IMPERSONATION_ID_NULL, archiveTransfer.getSrcSharedCtxGrantor());

        IRemoteDataClient srcClient = remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(),
                srcSystem, IMPERSONATION_ID_NULL, archiveTransfer.getSrcSharedCtxGrantor());

        TransferURI dstUri = new TransferURI(archiveTransfer.getDestinationBaseUrl());
        TapisSystem dstSystem = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache,
                systemsCacheNoAuth, permsService, opName, dstUri.getSystemId(), dstUri.getPath(),
                FileInfo.Permission.READ, IMPERSONATION_ID_NULL, archiveTransfer.getSrcSharedCtxGrantor());

        IRemoteDataClient dstClient = remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(),
                dstSystem, IMPERSONATION_ID_NULL, archiveTransfer.getSrcSharedCtxGrantor());

        //TODO AXFER: handle case of not FastXFER client
        if(srcClient instanceof ArchiveTransferSource srcArchiveXFer &&
           dstClient instanceof ArchiveTransferDestination dstArchiveXFer) {
            TransferURI srcBaseURI = new TransferURI(archiveTransfer.getSourceBaseUrl());
            TransferURI dstBaseURI = new TransferURI(archiveTransfer.getDestinationBaseUrl());
            TapisArchiveInputStream archiveInputStream =
                    srcArchiveXFer.getArchiveStream(srcBaseURI.getPath(), archiveTransfer.getRelativePaths());
            ArchiveTransferResult archiveTransferResult = dstArchiveXFer.writeArchive(dstBaseURI.getPath(), archiveInputStream);
            try {
                if(archiveTransferResult.isSuccess()) {
                    System.out.println("Successful");
                } else {
                    System.out.println("Failure");
                    System.out.println(archiveTransferResult);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private static ResourceRequestUser simulateResourceRequestUser(ArchiveTransfer archiveTransfer) {
        String oboUser = archiveTransfer.getUsername();
        String oboTenant = archiveTransfer.getTenantId();
        String jwtUser = TapisConstants.SERVICE_NAME_FILES;
        String jwtTenant = TransfersApp.getSiteAdminTenantId();
        return new ResourceRequestUser(new AuthenticatedUser(jwtUser, jwtTenant,
                TapisThreadContext.AccountType.service.name(), null, oboUser, oboTenant, null, null, null));
    }

}
