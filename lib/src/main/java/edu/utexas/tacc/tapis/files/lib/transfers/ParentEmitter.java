package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.providers.TenantCacheFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.tenants.client.TenantsClient;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

public class ParentEmitter {
    private Logger log = LoggerFactory.getLogger(ParentEmitter.class);

    private static void run(int count) throws ServiceException {

        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(RemoteDataClientFactory.class);
                bindAsContract(SystemsCache.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class);
                bindAsContract(SKClient.class);
                bindAsContract(SystemsClient.class);
                bindAsContract(TokensClient.class);
                bindAsContract(TenantsClient.class);
                bindFactory(ServiceJWTCacheFactory.class).to(ServiceJWT.class).in(Singleton.class);
                bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
            }
        });

        TransfersService transfersService = locator.getService(TransfersService.class);

      ResourceRequestUser rTestUser2 =
          new ResourceRequestUser(new AuthenticatedUser("testuser2", "dev", TapisThreadContext.AccountType.user.name(),
                                                        null, "testuser2", "dev", null, null, null));

      List<TransferTaskRequestElement> elements = new ArrayList<>();
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://dev.develop.tapis.io/tapis-demo/IMG_20200516_144049_1.jpg");
        element.setDestinationURI("tapis://dev.edvelop.tapis.io/tapis-demo/destFolder");
        elements.add(element);
        transfersService.createTransfer(rTestUser2, "testTag", elements);
    }

    public static void main(String[] args) throws Exception {
        run(1);
    }

}
