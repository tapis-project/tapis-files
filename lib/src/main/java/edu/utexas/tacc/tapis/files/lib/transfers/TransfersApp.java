package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.services.ChildTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.services.ParentTaskTransferService;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
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
public class TransfersApp
{
  // SSHConnection cache settings
  public static final long CACHE_MAX_SIZE = 1000;
  public static final long CACHE_TIMEOUT_MINUTES = 5;

  private static final Logger log = LoggerFactory.getLogger(TransfersApp.class);

  // We must be running on a specific site and this will never change
  private static String siteId;
  public static String getSiteId() {return siteId;}
  private static String siteAdminTenantId;
  public static String getSiteAdminTenantId() {return siteAdminTenantId;}

  public static void main(String[] args)
  {
    log.info("Starting transfers worker application.");

    // Initialize bindings for HK2 dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();

    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
      @Override
      protected void configure()
      {
        bindAsContract(FileOpsService.class).in(Singleton.class);
        bindAsContract(FileUtilsService.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bindAsContract(FileTransfersDAO.class);
        bindAsContract(RemoteDataClientFactory.class);
        bindAsContract(SystemsCache.class).in(Singleton.class);
        bindAsContract(SystemsCacheNoAuth.class).in(Singleton.class);
        bindAsContract(TransfersService.class).in(Singleton.class);
        bindAsContract(FilePermsService.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bindAsContract(ChildTaskTransferService.class).in(Singleton.class);
        bindAsContract(ParentTaskTransferService.class).in(Singleton.class);
        bindAsContract(FilePermsCache.class).in(Singleton.class);
        bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
      }
    });

    try {
      SshSessionPoolPolicy poolPolicy = SshSessionPoolPolicy.defaultPolicy()
              .setMaxConnectionDuration(Duration.ofHours(6))
              .setMaxConnectionIdleTime(Duration.ofMinutes(8))
              .setMaxConnectionsPerKey(RuntimeSettings.get().getSshPoolWorkerMaxConnectionsPerKey())
              .setMaxSessionsPerConnection(RuntimeSettings.get().getSshPoolWorkerMaxSessionsPerConnection())
              .setCleanupInterval(Duration.ofSeconds(15))
              .setTraceDuringCleanupFrequency(RuntimeSettings.get().getSshPoolTraceOnCleanupInterval())
              .setSessionCreationStrategy(SshSessionPoolPolicy.SessionCreationStrategy.MINIMIZE_CONNECTIONS);
      SshSessionPool.init(poolPolicy);

      // Get runtime parameters
      IRuntimeConfig runtimeConfig = RuntimeSettings.get();

      // Set site on which we are running. This is a required runtime parameter.
      siteId = runtimeConfig.getSiteId();

      // Init tenant manager, site admin tenant
      String url = runtimeConfig.getTenantsServiceURL();
      TenantManager tenantManager = TenantManager.getInstance(url);
      tenantManager.getTenants();
      // Set admin tenant also, needed when building a client for calling other services (such as SK) as ourselves.
      siteAdminTenantId = tenantManager.getSiteAdminTenantId(siteId);

      log.info("Getting serviceContext.");
      ServiceContext serviceContext = locator.getService(ServiceContext.class);
      log.info("Got serviceContext.");

      log.info("Getting childTxfrSvc.");
      ChildTaskTransferService childTaskTransferService = locator.getService(ChildTaskTransferService.class);
      log.info("Got childTxfrSvc.");
      log.info("Getting parentTxfrSvc.");
      ParentTaskTransferService parentTaskTransferService = locator.getService(ParentTaskTransferService.class);
      log.info("Got parentTxfrSvc.");
      log.info("Starting parent pipeline.");
      parentTaskTransferService.startListeners();
      log.info("Started parent pipeline.");

      log.info("Starting child pipeline.");
      childTaskTransferService.startListeners();
      log.info("Started child pipeline.");
    } catch(Exception ex) {
      String msg = LibUtils.getMsg("FILES_WORKER_APPLICATION_FAILED_TO_START", ex.getMessage());
      log.error(msg, ex);
    }
  }

  private static void logSuccess(TransferTask t) { log.info(t.toString()); }
  private static void logError(Throwable t) { log.error(t.getMessage(), t); }
}
