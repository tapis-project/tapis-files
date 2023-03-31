package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
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
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.providers.TenantCacheFactory;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import org.glassfish.hk2.api.ServiceHandle;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.internal.StarFilter;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TransfersApp
{
  // SSHConnection cache settings
  public static final long CACHE_MAX_SIZE = 1000;
  public static final long CACHE_TIMEOUT_MINUTES = 5;

  private static final Logger log = LoggerFactory.getLogger(TransfersApp.class);

  public static void main(String[] args)
  {
    log.info("Starting transfers worker application.");

    // Initialize bindings for HK2 dependency injection
    ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();

    ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
      @Override
      protected void configure()
      {
        bind(new SSHConnectionCache(CACHE_TIMEOUT_MINUTES, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
        bindAsContract(FileOpsService.class).in(Singleton.class);
        bindAsContract(FileUtilsService.class).in(Singleton.class);
        bindAsContract(FileShareService.class).in(Singleton.class);
        bindAsContract(FileTransfersDAO.class);
        bindAsContract(RemoteDataClientFactory.class);
        bindAsContract(SystemsCache.class).in(Singleton.class);
        bindAsContract(SystemsCacheNoAuth.class).in(Singleton.class);
        bindAsContract(TransfersService.class).in(Singleton.class);
        bindAsContract(FilePermsService.class).in(Singleton.class);
        bindAsContract(ChildTaskTransferService.class).in(Singleton.class);
        bindAsContract(ParentTaskTransferService.class).in(Singleton.class);
        bindAsContract(FilePermsCache.class).in(Singleton.class);
        bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
        bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
        bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
      }
    });

    try {
      // Need to init the tenant manager for some reason.
      TenantManager tenantManager = locator.getService(TenantManager.class);
      tenantManager.getTenants();
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
      Flux<TransferTaskParent> parentTaskFlux = parentTaskTransferService.runPipeline();
      parentTaskFlux.subscribe();
      log.info("Started parent pipeline.");

      log.info("Starting child pipeline.");
      Flux<TransferTaskChild> childTaskFlux = childTaskTransferService.runPipeline();
      childTaskFlux.subscribe();
      log.info("Started child pipeline.");
    } catch(Exception ex) {
      String msg = LibUtils.getMsg("FILES_WORKER_APPLICATION_FAILED_TO_START", ex.getMessage());
      log.error(msg, ex);
    }
  }

  private static void logSuccess(TransferTask t) { log.info(t.toString()); }
  private static void logError(Throwable t) { log.error(t.getMessage(), t); }
}
