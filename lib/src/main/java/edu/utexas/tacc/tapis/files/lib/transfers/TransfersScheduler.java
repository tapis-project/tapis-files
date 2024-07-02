package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.TransferWorkerDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.services.ChildTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.services.ParentTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.providers.TenantCacheFactory;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPoolPolicy;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
public class TransfersScheduler
{
    private static final Logger log = LoggerFactory.getLogger(TransfersApp.class);
    private static int WORKER_BACKLOG_THRESHOLD = 100;
    private static int ROW_NUMBER_CUTOFF = 3;
    private static long EXPECT_HEARTBEAT_BEFORE_MILLIS = 300000;

    FileTransfersDAO dao = new FileTransfersDAO();
    TransferWorkerDAO transferWorkerDAO = new TransferWorkerDAO();

    public static void main(String[] args)
    {
        log.info("Starting transfers scheduler.");
        try {
            checkRequiredSettings();
            TransfersScheduler scheduler = new TransfersScheduler();
            scheduler.run();
        } catch(Exception ex) {
            // TODO:  Add message to catalog
            String msg = LibUtils.getMsg("FILES_TRANSFER_SCHEDULER_APPLICATION_FAILED_TO_START", ex.getMessage());
            log.error(msg, ex);
        }
    }

    void run() {
//        List<TransferWorker> transferWorkers = new ArrayList<TransferWorker>();
//        for(;;) {
//            updateWorkerList(transferWorkers);
//            scheduleWork();
//            politePause();
//        }
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
            throw new RuntimeException(MsgUtils.getMsg("FILES_TRANSFER_SCHEDULER_SERVICE_MISSING_REQUIRED_VARIABLES", missingVars.toString()));
        }
    }

    void updateWorkerList(Map<UUID, TransferWorker> workerMap) {
        List<TransferWorker> workers = null;
        try(DAOTransactionContext context = new DAOTransactionContext()) {
            workers = transferWorkerDAO.getTransferWorkers(context);
        } catch (DAOException ex) {

        }

        for(TransferWorker worker : workers) {
            TransferWorker transferWorker = workerMap.get(worker.getUuid());
            if ((transferWorker == null) &&
                    (Instant.now().minusMillis(EXPECT_HEARTBEAT_BEFORE_MILLIS).isAfter(worker.getLastUpdated()))) {
                // this is expired.  Remove it from the map and do some cleanup
                workerMap.remove(worker.getUuid());
                cleanupWorkerTasks(worker.getUuid());
            } else {
                workerMap.put(worker.getUuid(), worker);
            }
        }
    }

    private void cleanupWorkerTasks(UUID workerTasks) {

    }

}
