package edu.utexas.tacc.tapis.files.lib.services;

import com.google.common.base.Stopwatch;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveInputPipe;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferDestination;
import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveTransferLog;
import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveTransferProvider;
import edu.utexas.tacc.tapis.files.lib.transfers.ArchiveTransferResult;
import edu.utexas.tacc.tapis.files.lib.clients.ArchiveTransferSource;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.transfers.ObservableArchiveInputStream;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.transfers.ToArchiveTransferResult;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.ArchiveTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.exceptions.SchedulingPolicyException;
import edu.utexas.tacc.tapis.files.lib.exceptions.UnrecoverableTransferException;
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
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
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
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
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
public class ArchiveTransferWorker {
    private ScheduledExecutorService archiveTransferScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        @Override
        public Thread newThread(@NotNull Runnable runnable) {
            Thread th = defaultFactory.newThread(runnable);
            th.setDaemon(true);
            th.setName("ArchiveTransferWorker - " + th.getName());
            return th;
        }
    });

    private static final int MAX_THREADS = RuntimeSettings.get().getArchiveTransferThreadPoolSize();
    private static final TemporalAmount RETRY_WAIT = Duration.ofMinutes(10);
    private static final Logger log = LoggerFactory.getLogger(ArchiveTransferWorker.class);

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
                                    Future<ArchiveTransferResult> future = archiveTransferWorkers.submit(new Callable<ArchiveTransferResult>() {
                                        @Override
                                        public ArchiveTransferResult call() throws Exception {
                                            Stopwatch sw = Stopwatch.createStarted();
                                            ArchiveTransferResult archiveTransferResult = doTransfer(archiveTransferUuid);
                                            log.trace("ARCHIVE TRANSFER TIMING: ArchiveTransfer callable Id: " + prioritizedArchiveTransfer.getObject().getId() + " time: " + sw.elapsed(TimeUnit.MILLISECONDS));
                                            return archiveTransferResult;
                                        }
                                    });
                                    futures.put(archiveTransferUuid, future);
                                }
                            }
                        } catch (SchedulingPolicyException ex) {
                            log.error(LibUtils.getMsg("FILES_TXFR_SVC_ERROR_GETTING_WORK", myUuid));
                            break;
                        }
                        Thread.yield();
                        if (futures.isEmpty()) {
                            shouldExit = true;
                        }
                    }
                } catch (Throwable th) {
                    // if this method throws, it will not get rescheduled.  We would have a zombie worker.  I think the
                    // best thing to do here is exit - we have caught some completely unexpected exception
                    log.error("Fatal Error.  Exiting worker", th);
                    System.exit(0);
                }
                Thread.yield();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private boolean canCreateNewFutures(Map<UUID, Future<ArchiveTransferResult>> futures, int capacity) throws DAOException {
        // remove all completed transfers before checking capacity
        for (UUID key : futures.keySet()) {
            Future<ArchiveTransferResult> resultFuture = futures.get(key);
            // updateArchiveTransferIfComplete returns true if it was complete, or false if it's still in progress
            if(updateArchiveTransferIfComplete(key, resultFuture)) {
                // remove the future if its result's future(s) are complete
                futures.remove(key);
            }
        }

        return futures.size() < capacity;
    }

    private ArchiveTransferResult doTransfer(UUID archiveTransferUuid) throws IOException, DAOException {
        final String opName = "doTransfer";

        // get the resourceRequestUser
        ArchiveTransfersDAO dao = new ArchiveTransfersDAO();
        ArchiveTransfer archiveTransfer = DAOTransactionContext.doInTransaction(context -> {
            ArchiveTransfer currentTransfer = dao.getArchiveTransfer(context, archiveTransferUuid, true, true, false);
            currentTransfer.setStatus(ArchiveTransferStatus.IN_PROGRESS);
            currentTransfer.setStartTime(Instant.now());
            return dao.updateArchiveTransfer(context, currentTransfer, true, false);
        });

        ResourceRequestUser rUser = simulateResourceRequestUser(archiveTransfer);
        ArchiveTransferParams params = getArchiveTransferParams(rUser, archiveTransfer);
        validateParams(params);

        IRemoteDataClient srcClient = remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(),
                params.getSrcSystem(), IMPERSONATION_ID_NULL, params.getSrcSharedCtxGrantor());

        IRemoteDataClient dstClient = remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(),
                params.getDstSystem(), IMPERSONATION_ID_NULL, params.getSrcSharedCtxGrantor());

        MessageDigest sha256Digest = null;
        try {
            sha256Digest = MessageDigest.getInstance("SHA256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ArchiveTransferResult archiveTransferResult = switch (params.archiveType) {
            case TAR, TAR_GZIP -> {
                if (srcClient instanceof ArchiveTransferSource srcArchiveXFer &&
                        dstClient instanceof ArchiveTransferDestination dstArchiveXFer) {
                    yield handleFullTransfer(srcArchiveXFer, dstArchiveXFer, params, sha256Digest);
                }
                throw new UnrecoverableTransferException(LibUtils.getMsg("FILES_XFER_INVALID_FULL_ARCHIVE_NOT_SUPPORTED", opName));
            }

            case TAR_ARCHIVE, GZIP_ARCHIVE -> {
                if (srcClient instanceof ArchiveTransferSource srcArchiveXFer) {
                    yield handleToArchiveTransfer(srcArchiveXFer, dstClient, params, sha256Digest);
                }
                throw new UnrecoverableTransferException(LibUtils.getMsg("FILES_XFER_INVALID_TO_ARCHIVE_NOT_SUPPORTED", opName));

            }
            case EXPAND_TAR_ARCHIVE, EXPAND_GZIP_ARCHIVE -> {
                if (dstClient instanceof ArchiveTransferDestination dstArchiveXFer) {
                    yield handleFromArchiveTransfer(srcClient, dstArchiveXFer, params, sha256Digest);
                }
                throw new UnrecoverableTransferException(LibUtils.getMsg("FILES_XFER_INVALID_FROM_ARCHIVE_NOT_SUPPORTED", opName));
            }
        };

        return archiveTransferResult;
    }

    private ArchiveTransferResult handleFullTransfer(ArchiveTransferSource srcClient, ArchiveTransferDestination dstClient, ArchiveTransferParams params, MessageDigest md) throws IOException {
        ArchiveTransferProvider archiveTransferProvider = new ArchiveTransferProvider(params.getArchiveType(), md);

        ArchiveInputPipe archiveInputPipe = srcClient.getArchiveStream(
                params.getSrcUri().getPath(), params.getRelativePaths(), archiveTransferProvider);
        ObservableArchiveInputStream observableArchiveInputStream = new ObservableArchiveInputStream(archiveInputPipe, archiveTransferProvider);
        ArchiveTransferLog archiveTransferLog = new ArchiveTransferLog();
        observableArchiveInputStream.addObserver(archiveTransferLog);
        ArchiveTransferResult archiveTransferResult = dstClient.writeArchive(
                params.getDstUri().getPath(), observableArchiveInputStream,
                archiveTransferProvider, archiveInputPipe.getSourceResultFuture());
        archiveTransferResult.setArchiveTransferLog(archiveTransferLog);

        return archiveTransferResult;
    }

    private ArchiveTransferResult handleToArchiveTransfer(ArchiveTransferSource srcClient, IRemoteDataClient dstClient, ArchiveTransferParams params, MessageDigest md) throws IOException {
        ArchiveTransferProvider archiveTransferProvider = new ArchiveTransferProvider(params.getArchiveType(), md);

        ArchiveInputPipe archiveInputPipe = srcClient.getArchiveStream(
                params.getSrcUri().getPath(), params.getRelativePaths(), archiveTransferProvider);
        ObservableArchiveInputStream observableArchiveInputStream = new ObservableArchiveInputStream(archiveInputPipe, archiveTransferProvider);
        ArchiveTransferLog archiveTransferLog = new ArchiveTransferLog();
        observableArchiveInputStream.addObserver(archiveTransferLog);
        ArchiveTransferResult archiveTransferResult = new ToArchiveTransferResult(archiveInputPipe.getSourceResultFuture());
        archiveTransferResult.setArchiveTransferLog(archiveTransferLog);
        dstClient.upload(params.dstUri.getPath(), observableArchiveInputStream);

        return archiveTransferResult;
    }

    private ArchiveTransferResult handleFromArchiveTransfer(IRemoteDataClient srcClient, ArchiveTransferDestination dstClient, ArchiveTransferParams params, MessageDigest md) throws IOException {
        final String opName = "handleFromArchiveTransfer";
        ArchiveTransferProvider archiveTransferProvider = new ArchiveTransferProvider(params.getArchiveType(), md);

        String srcPath = params.getSrcUri().getPath();

        // for now, we will not expand symlink'ed archives ... but maybe that should change?
        FileInfo info = srcClient.getFileInfo(srcPath, false);
        if((info == null) || (!info.isFile())) {
            throw new UnrecoverableTransferException(LibUtils.getMsg("FILES_XFER_INVALID_PARAMETER",
                    opName, "sourceURI", params.getSrcUri()));
        }

        InputStream inputStream = srcClient.getStream(srcPath);
        ObservableArchiveInputStream observableArchiveInputStream = new ObservableArchiveInputStream(inputStream, archiveTransferProvider);
        ArchiveTransferLog archiveTransferLog = new ArchiveTransferLog();
        observableArchiveInputStream.addObserver(archiveTransferLog);

        ArchiveTransferResult archiveTransferResult = dstClient.writeArchive(params.getDstUri().getPath(),
                observableArchiveInputStream, archiveTransferProvider);
        archiveTransferResult.setArchiveTransferLog(archiveTransferLog);

        return archiveTransferResult;
    }


    private ArchiveTransferResult handleFromArchiveTransfer() {
        return null;
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
        params.setArchiveType(ArchiveTransferProvider.ArchiveType.valueOf(archiveTransfer.getArchiveType()));

        return params;
    }

    private void validateParams(ArchiveTransferParams params) {
        if((!params.getSrcUri().isTapisProtocol()) || (!params.getDstUri().isTapisProtocol()))  {
            String msg = MsgUtils.getMsg("FILES_XFER_INVALID_PARAMETER", "validateParams",
                    "src/destination protocol", params.getSrcUri().getPath() + "/"  + params.getDstUri().getProtocol());
            throw new UnrecoverableTransferException(msg);
        }
    }

    private boolean updateArchiveTransferIfComplete(UUID archiveTransferUuid, Future<ArchiveTransferResult> resultFuture) throws DAOException {
        ArchiveTransferResult result = null;
        String errorMessage = null;

        try {
            result = resultFuture.get();
            if(!result.isComplete()) {
                return false;
            }

            // NOTE - "waitForCompletion" is the thing that waits for the futures contained
            // inside of "resultFuture" to complete.  It has to be here - see waitForCompletion()
            // for details.
            result.waitForCompletion();
            errorMessage = result.getMessages();

            if((result != null) && (!result.isSuccess())) {
                scheduleRetryOrFail(archiveTransferUuid, errorMessage, false);
                return true;
            }
        } catch (Throwable th) {
            if(th instanceof ExecutionException executionException) {
                Throwable cause = executionException.getCause();
                while(cause != null) {
                    if(cause instanceof UnrecoverableTransferException unrecoverableTransferException) {
                        // force failure - this is unrecoverable
                        scheduleRetryOrFail(archiveTransferUuid, cause.getMessage(), true);
                        break;
                    }
                    cause = cause.getCause();
                }

            }
            scheduleRetryOrFail(archiveTransferUuid, th.getMessage(), false);
            return true;
        }

        // if we've gotten this far, the transfer was successfull, so mark it complete
        ArchiveTransferLog archiveTransferLog = result.getArchiveTransferLog();
        final long fileBytesRead = (archiveTransferLog == null) ? 0 : archiveTransferLog.getFileBytesRead();
        final long archiveBytesRead = (archiveTransferLog == null) ? 0 : archiveTransferLog.getArchiveBytesRead();

        final String updateErrorMessage = errorMessage;
        ArchiveTransfersDAO dao = new ArchiveTransfersDAO();

        DAOTransactionContext.doInTransaction(context -> {
            ArchiveTransfer currentTransfer = dao.getArchiveTransfer(context, archiveTransferUuid, true, true, false);
            currentTransfer.setStatus(ArchiveTransferStatus.COMPLETED);
            currentTransfer.setErrorMessage(updateErrorMessage);
            currentTransfer.setArchiveBytesRead(archiveBytesRead);
            currentTransfer.setEndTime(Instant.now());
            currentTransfer.setFileBytesRead(fileBytesRead);
            currentTransfer.setNextRetry(null);
            currentTransfer.setRetriesRemaining(0);
            currentTransfer.setTransferLogEntries(archiveTransferLog.getLogEntries());
            return dao.updateArchiveTransfer(context, currentTransfer, false, true);
        });

        return true;
    }

    private ArchiveTransfer scheduleRetryOrFail(UUID archiveTransferUuid, String errorMessage, boolean forceFail) throws DAOException {
        ArchiveTransfersDAO dao = new ArchiveTransfersDAO();

        return DAOTransactionContext.doInTransaction(context -> {
            // read for update
            ArchiveTransfer currentTransfer = dao.getArchiveTransfer(context, archiveTransferUuid, true, true, false);

            // if it's already in a 'final' state, ignore this request and return.
            if(currentTransfer.getStatus().isFinalState()) {
                return currentTransfer;
            }

            int retriesRemaining = currentTransfer.getRetriesRemaining();
            StringBuilder errorMessageBuilder = new StringBuilder();
            if ((retriesRemaining > 0) && (!forceFail)) {
                // if there are more retries, schedule the next one.
                currentTransfer.setRetriesRemaining(retriesRemaining - 1);
                currentTransfer.setStatus(ArchiveTransferStatus.AWAITING_RETRY);
                currentTransfer.setNextRetry(Instant.now().plus(RETRY_WAIT));
                errorMessageBuilder.append("Scheduling retry:  ");
                errorMessageBuilder.append(System.lineSeparator());
                errorMessageBuilder.append(errorMessage);
            } else {
                // if there are no more retries, fail the transfer
                currentTransfer.setStatus(ArchiveTransferStatus.FAILED);
                currentTransfer.setRetriesRemaining(0);
                currentTransfer.setNextRetry(null);
                errorMessageBuilder.append("No more retries available.  Last error:");
                errorMessageBuilder.append(System.lineSeparator());
                errorMessageBuilder.append(errorMessage);
            }
            currentTransfer.setErrorMessage(errorMessageBuilder.toString());
            currentTransfer.setAssignedTo(null);

            return dao.updateArchiveTransfer(context, currentTransfer, false, false);
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
