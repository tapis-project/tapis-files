package edu.utexas.tacc.tapis.files.lib.services;

import com.google.common.base.Stopwatch;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveInputPipe;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferDestination;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferLog;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferResult;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferSource;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.ObservableTapisArchiveInputStream;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransfer;
import edu.utexas.tacc.tapis.files.lib.models.ArchiveTransferStatus;
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
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
            th.setName("ArchiveTransferThread: " + th.getName());
            return th;
        }
    });



    public void start(UUID myUuid) {
        Map<UUID, Future<ArchiveTransferResult>> futures = new ConcurrentHashMap<UUID, Future<ArchiveTransferResult>>();

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
                                        Future<ArchiveTransferResult> future = archiveTransferWorkers.submit(new Callable<ArchiveTransferResult>() {
                                            @Override
                                            public ArchiveTransferResult call() throws Exception {
                                                Stopwatch sw = Stopwatch.createStarted();
                                                try {
                                                    return doTransfer(prioritizedArchiveTransfer.getObject());
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
                        if (futures.isEmpty()) {
                            shouldExit = true;
                        }
                    }
                } catch (Throwable th) {
                    // if this method throws, it will not get rescheduled.  We would have a zombie worker.  I think the
                    // best thing to do here is exit - we have caught some completely unexpected exception
                    System.out.println(th);
                    System.exit(0);
                }
                Thread.yield();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private boolean canCreateNewFutures(Map<UUID, Future<ArchiveTransferResult>> futures, int capacity) throws DAOException {
//        if(futures.size() >= capacity) {
            for (UUID key : futures.keySet()) {
                Future<ArchiveTransferResult> resultFuture = futures.get(key);
                if (resultFuture.isDone()) {
                    updateArchiveTransfer(key, resultFuture);
                    futures.remove(key);
                }
            }
//        }

        return futures.size() < capacity;
    }

    private ArchiveTransferResult doTransfer(ArchiveTransfer archiveTransfer) throws IOException, DAOException {
        // TODO AXFER:  I think this should be passed in - not the whole archiveTransfer
        UUID archiveTransferUuid = archiveTransfer.getUuid();

        // get the resourceRequestUser
        ArchiveTransfersDAO dao = new ArchiveTransfersDAO();
        archiveTransfer = DAOTransactionContext.doInTransaction(context -> {
            ArchiveTransfer currentTransfer = dao.getArchiveTransfer(context, archiveTransferUuid, true, true);
            currentTransfer.setStatus(ArchiveTransferStatus.IN_PROGRESS);
            return dao.updateArchiveTransfer(context, currentTransfer, true);
        });

        ResourceRequestUser rUser = simulateResourceRequestUser(archiveTransfer);
        ArchiveTransferParams params = getArchiveTransferParams(rUser, archiveTransfer);
        validateParams(params);
        return handleTransfer(rUser, params);
    }

    private ArchiveTransferParams getArchiveTransferParams(ResourceRequestUser rUser, ArchiveTransfer archiveTransfer) {
        ArchiveTransferParams params = new ArchiveTransferParams();
        TransferURI srcUri = new TransferURI(archiveTransfer.getSourceBaseUrl());
        params.setSrcUri(srcUri);
        TapisSystem srcSystem = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache,
                systemsCacheNoAuth, permsService, "archiveTransfer", srcUri.getSystemId(), srcUri.getPath(),
                FileInfo.Permission.READ, IMPERSONATION_ID_NULL, archiveTransfer.getSrcSharedCtxGrantor());
        params.setSrcSystem(srcSystem);
        params.setSrcSharedCtxGrantor(archiveTransfer.getSrcSharedCtxGrantor());

        TransferURI dstUri = new TransferURI(archiveTransfer.getDestinationBaseUrl());
        params.setDstUri(dstUri);
        TapisSystem dstSystem = LibUtils.getResolvedSysWithAuthCheck(rUser, shareService, systemsCache,
                systemsCacheNoAuth, permsService, "archiveTransfer", dstUri.getSystemId(), dstUri.getPath(),
                FileInfo.Permission.READ, IMPERSONATION_ID_NULL, archiveTransfer.getSrcSharedCtxGrantor());
        params.setDstSystem(dstSystem);
        params.setDstSharedCtxGrantor(archiveTransfer.getDestSharedCtxGrantor());

        params.setRelativePaths(archiveTransfer.getRelativePaths());
        params.setCompress(archiveTransfer.getCompress());

        return params;
    }

    private void validateParams(ArchiveTransferParams params) {
        if((!params.getSrcUri().isTapisProtocol()) || (!params.getDstUri().isTapisProtocol()))  {
            // TODO AXFER: think about exceptiosn a bit!! - not just here but everywehere in AXFER
            throw new RuntimeException("Error - must be tapis protocol");
        }
    }

    private ArchiveTransferResult handleTransfer(ResourceRequestUser rUser, ArchiveTransferParams params) throws IOException {
        final String opName = "processTransfer";

        IRemoteDataClient srcClient = remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(),
                params.getSrcSystem(), IMPERSONATION_ID_NULL, params.getSrcSharedCtxGrantor());

        IRemoteDataClient dstClient = remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(),
                params.getDstSystem(), IMPERSONATION_ID_NULL, params.getSrcSharedCtxGrantor());

        ArchiveTransferResult result = null;
        //TODO AXFER: handle case of not FastXFER client
        ArchiveTransferResult archiveTransferResult = null;

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        if(srcClient instanceof ArchiveTransferSource srcArchiveXFer &&
           dstClient instanceof ArchiveTransferDestination dstArchiveXFer) {
            ArchiveInputPipe archiveInputPipe = srcArchiveXFer.getArchiveStream(
                    params.getSrcUri().getPath(), params.getRelativePaths(), params.getCompress());
// with observeable stream
                ObservableTapisArchiveInputStream observableTapisArchiveInputStream = new ObservableTapisArchiveInputStream(archiveInputPipe, md, params.compress);
//                ArchiveTransferLog archiveTransferLog = new ArchiveTransferLog();
//                observableTapisArchiveInputStream.addObserver(archiveTransferLog);
//                archiveTransferResult = dstArchiveXFer.writeArchive(params.getDstUri().getPath(), observableTapisArchiveInputStream, params.getCompress(), archiveInputPipe.getSourceResultFuture());
//                archiveTransferResult.setArchiveTransferLog(archiveTransferLog);


// without observeable stream
            archiveTransferResult = dstArchiveXFer.writeArchive(params.getDstUri().getPath(),
                    archiveInputPipe, params.getCompress(), archiveInputPipe.getSourceResultFuture());
        }

        return archiveTransferResult;
    }

    private void updateArchiveTransfer(UUID archiveTransferUuid, Future<ArchiveTransferResult> resultFuture) throws DAOException {
        ArchiveTransferResult result = null;
        String errorMessage = null;
        ArchiveTransferStatus status = null;

        try {
            result = resultFuture.get();
            result.waitForCompletion();

            if((result != null) && (result.isSuccess())) {
                status = ArchiveTransferStatus.COMPLETED;
            } else {
                status = ArchiveTransferStatus.FAILED;
            }
            errorMessage = result.getMessages();
            //TODO AXFER: update the task with success/fail include message if failed
        } catch (Throwable th) {
            //TODO AXFER: update the task with fail - include exception text
            errorMessage = th.getMessage();
            status = ArchiveTransferStatus.FAILED;
        }

        ArchiveTransferLog archiveTransferLog = result.getArchiveTransferLog();
        final long fileBytesRead = (archiveTransferLog == null) ? 0 : archiveTransferLog.getFileBytesRead();
        final long archiveBytesRead = (archiveTransferLog == null) ? 0 : archiveTransferLog.getArchiveBytesRead();

        final String updateErrorMessage = errorMessage;
        ArchiveTransferStatus updateStatus = status;
        ArchiveTransfersDAO dao = new ArchiveTransfersDAO();
        ArchiveTransfer archiveTransfer = DAOTransactionContext.doInTransaction(context -> {
            ArchiveTransfer currentTransfer = dao.getArchiveTransfer(context, archiveTransferUuid, true, true);
            currentTransfer.setErrorMessage(updateErrorMessage);
            currentTransfer.setStatus(updateStatus);
            currentTransfer.setArchiveBytesRead(archiveBytesRead);
            currentTransfer.setFileBytesRead(fileBytesRead);
            currentTransfer.setFileBytesRead(fileBytesRead);
            return dao.updateArchiveTransfer(context, currentTransfer, false);
        });
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
