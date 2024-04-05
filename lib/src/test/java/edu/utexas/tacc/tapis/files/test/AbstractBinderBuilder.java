package edu.utexas.tacc.tapis.files.test;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.caches.TenantAdminCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.postits.PostItsDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.services.PostItsService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import javax.inject.Singleton;
import java.util.Map;

public class AbstractBinderBuilder {
    private ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();

    private FilePermsService permsMock = null;
    private SystemsCache systemsCacheMock = null;
    private SystemsCacheNoAuth systemsCacheNoAuthMock = null;


    public AbstractBinderBuilder mockPerms(FilePermsService permsMock) {
        this.permsMock = permsMock;
        return this;
    }

    public AbstractBinderBuilder mockSystemsCache(SystemsCache systemsCacheMock) {
        this.systemsCacheMock = systemsCacheMock;
        return this;
    }

    public AbstractBinderBuilder mockSystemsCacheNoAuth(SystemsCacheNoAuth systemsCacheNoAuthMock) {
        this.systemsCacheNoAuthMock = systemsCacheNoAuthMock;
        return this;
    }

    public AbstractBinder build() {
        return new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(FileOpsService.class).in(Singleton.class);
                bindAsContract(FileUtilsService.class).in(Singleton.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class);
                if(systemsCacheMock == null) {
                    bindAsContract(SystemsCache.class).in(Singleton.class);
                } else {
                    bind(systemsCacheMock).to(SystemsCache.class).ranked(1);
                }
                if(systemsCacheNoAuthMock == null) {
                    bindAsContract(SystemsCacheNoAuth.class).in(Singleton.class);
                } else {
                    bind(systemsCacheNoAuthMock).to(SystemsCacheNoAuth.class).ranked(1);
                }
                if(permsMock == null) {
                    bindAsContract(FilePermsService.class).in(Singleton.class);
                } else {
                    bind(permsMock).to(FilePermsService.class).ranked(1);
                }
                bindAsContract(FilePermsCache.class).in(Singleton.class);
                bindAsContract(TenantAdminCache.class).in(Singleton.class);
                bindAsContract(FileShareService.class).in(Singleton.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bindAsContract(PostItsService.class).in(Singleton.class);
                bindAsContract(PostItsDAO.class).in(Singleton.class);
                bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
                bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
            }

        };
    }

    private void initTenantManager() {
        // Initialize TenantManager
        IRuntimeConfig settings = RuntimeSettings.get();
        String url = settings.getTenantsServiceURL();
        Map<String, Tenant> tenants = TenantManager.getInstance(url).getTenants();
    }

    public ServiceLocator buildAsServiceLocator() {
        initTenantManager();

        ServiceLocatorUtilities.bind(locator, build());

        // for some reaons things don't get setup right if other stuff gets requested from the locator
        // before this does
        locator.getService(ServiceContext.class);

        return locator;
    }
}
