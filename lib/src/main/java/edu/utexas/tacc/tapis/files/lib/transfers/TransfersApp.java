package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.providers.TenantCacheFactory;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import org.glassfish.hk2.api.Immediate;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.AcknowledgableDelivery;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

public class TransfersApp {

    private static Logger log = LoggerFactory.getLogger(TransfersApp.class);

    public static void main(String[] args) throws Exception {

        log.info("Starting transfers application.");

        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(RemoteDataClientFactory.class);
                bindAsContract(SystemsCache.class).in(Singleton.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class).in(Singleton.class);
                bindAsContract(FilePermsService.class).in(Singleton.class);
                bindAsContract(FilePermsCache.class).in(Singleton.class);
                bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
                bind(new SSHConnectionCache(60, TimeUnit.SECONDS)).to(SSHConnectionCache.class);
                bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
                bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
                bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
            }
        });

        // TODO
        // Need to init the tenant manager for some reason.
        TenantManager tenantManager = locator.getService(TenantManager.class);
        tenantManager.getTenants();
        ServiceContext serviceContext = locator.getService(ServiceContext.class);

        TransfersService transfersService = locator.getService(TransfersService.class);

        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTaskParent> parentTaskFlux = transfersService.processParentTasks(parentMessageStream);
        parentTaskFlux.subscribe();

        Flux<AcknowledgableDelivery> childMessageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> childTaskFlux = transfersService.processChildTasks(childMessageStream);
        childTaskFlux.subscribe();

    }

    private static void logSuccess(TransferTask t) {
        log.info(t.toString());
    }

    private static void logError(Throwable t) {
        log.error(t.getMessage(), t);
    }


}
