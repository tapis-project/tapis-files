package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.AcknowledgableDelivery;

import javax.inject.Singleton;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TransfersApp {

    private static Logger log = LoggerFactory.getLogger(TransfersApp.class);

    public static void main(String[] args) throws Exception {

        log.info("Starting transfers application");

        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bind(RemoteDataClientFactory.class).to(IRemoteDataClient.class);
                bindAsContract(RemoteDataClientFactory.class);
                bindAsContract(TransfersService.class);
                bindAsContract(SystemsClientFactory.class);
                bindAsContract(FileTransfersDAO.class);
                bind(new SSHConnectionCache(2, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindFactory(ServiceJWTCacheFactory.class).to(ServiceJWT.class).in(Singleton.class);
                bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
            }
        });

        TransfersService transfersService = locator.getService(TransfersService.class);
        transfersService.setChildQueue(UUID.randomUUID().toString());
        transfersService.setParentQueue(UUID.randomUUID().toString());


        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTaskParent> parentTaskFlux = transfersService.processParentTasks(parentMessageStream);
        parentTaskFlux.subscribe();

        Flux<AcknowledgableDelivery> childMessageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> childTaskFlux = transfersService.processChildTasks(childMessageStream);
        childTaskFlux.subscribe();

    }

    private void logSuccess(TransferTask t) {
        log.info(t.toString());
    }

    private void logError(Throwable t) {
        log.error(t.getMessage(), t);
    }


}
