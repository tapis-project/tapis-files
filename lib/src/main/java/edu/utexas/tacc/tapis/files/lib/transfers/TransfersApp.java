package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

public class TransfersApp {

    private static Logger log = LoggerFactory.getLogger(TransfersApp.class);


    public static void main(String[] args) throws Exception {
        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bind(RemoteDataClientFactory.class).to(IRemoteDataClient.class);
                bindAsContract(RemoteDataClientFactory.class);
                bindAsContract(TransfersService.class);
                bindAsContract(SystemsClientFactory.class);
                bindAsContract(ParentTaskReceiver.class);
                bindAsContract(ChildTaskReceiver.class);
                bindAsContract(FileTransfersDAO.class);
                bind(new SSHConnectionCache(2, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindFactory(ServiceJWTCacheFactory.class).to(ServiceJWT.class).in(Singleton.class);
                bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
            }
        });
        ParentTaskReceiver worker = locator.getService(ParentTaskReceiver.class);
//        Thread parentThread = new Thread(worker);
//        parentThread.start();
//
//
//        log.info("test");
//        ChildTaskReceiver childWorker = locator.getService(ChildTaskReceiver.class);
//        Thread childThread = new Thread(childWorker);
//        childThread.start();


    }

}
