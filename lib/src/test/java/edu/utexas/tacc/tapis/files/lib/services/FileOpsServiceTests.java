package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream;
import edu.utexas.tacc.tapis.files.test.RandomByteInputStream.SizeUnit;
import edu.utexas.tacc.tapis.files.test.TestUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.inject.Singleton;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@Test
public class FileOpsServiceTests {
    private static final Logger log  = LoggerFactory.getLogger(TestFileOpsService.class);
    private static final String JSON_TEST_PATH="edu/utexas/tacc/tapis/files/lib/clients/TestSystems.json";
    private static String devTenant = "dev";
    private static String testuser = "testuser";

    public ServiceLocator setupServiceLocator()
    {
        // Initialize TenantManager
        IRuntimeConfig settings = RuntimeSettings.get();
        String url = settings.getTenantsServiceURL();
        Map<String, Tenant> tenants = TenantManager.getInstance(url).getTenants();

        // Setup for dependency injection
        FilePermsService permsService = Mockito.mock(FilePermsService.class);
        SystemsCache systemsCache = Mockito.mock(SystemsCache.class);
        SystemsCacheNoAuth systemsCacheNoAuth = Mockito.mock(SystemsCacheNoAuth.class);
        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bindAsContract(FileOpsService.class).in(Singleton.class);
                bindAsContract(FileShareService.class).in(Singleton.class);
                bind(systemsCache).to(SystemsCache.class).ranked(1);
                bind(systemsCacheNoAuth).to(SystemsCacheNoAuth.class).ranked(1);
                bind(permsService).to(FilePermsService.class).ranked(1);
                bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
                bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
            }
        });

        return locator;
    }

    @BeforeSuite
    public void beforeSuite() {
        SshSessionPool.init();
    }

    // Data provider for all test systems except S3
    // Needed since S3 does not support hierarchical directories and we do not emulate such functionality
    @DataProvider(name= "testSystems")
    public Object[][] testSystemsNoS3DataProvider () throws IOException {
        Map<String, TapisSystem> tapisSystemMap = TestUtils.readSystems(JSON_TEST_PATH);
        Object[][] providedData = new Object[tapisSystemMap.size()][1];
        int i = 0;
        for(String systemName : tapisSystemMap.keySet()) {
            providedData[i][0] = tapisSystemMap.get(systemName);
            i++;
        };
        return providedData;
    }

    @Test(dataProvider = "testSystems")
    public void testListingPath(TapisSystem testSystem) throws Exception
    {
        /*

        ServiceLocator locator = new LocatorBuilder()
                .configurePermsMock(TestUtils.getAllowAllMock(devTenant, testuser, testSystem)).build();

        FileOpsService fileOpsService = locator.getService(FileOpsService.class);
       */
        // getServiceLocator
        ServiceLocator locator = setupServiceLocator();

        // get mocked services, and reset
        FilePermsService permsService = locator.getService(FilePermsService.class);
        SystemsCache systemsCache = locator.getService(SystemsCache.class);
        SystemsCacheNoAuth systemsCacheNoAuth = locator.getService(SystemsCacheNoAuth.class);
        reset(permsService, systemsCache, systemsCacheNoAuth);

        // setup mocks
        when(permsService.isPermitted(eq(devTenant), eq(testuser), eq(testSystem.getId()), any(), any())).thenReturn(true);
        when(systemsCache.getSystem(devTenant, testSystem.getId(), testuser, null, null)).thenReturn(testSystem);
        when(systemsCacheNoAuth.getSystem(devTenant, testuser, testSystem.getId())).thenReturn(testSystem);

        // get service to test
        FileOpsService fileOpsService = locator.getService(FileOpsService.class);

        RemoteDataClientFactory remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testuser, testSystem);

        RandomByteInputStream inputStream = new RandomByteInputStream(1024, SizeUnit.BYTES, true);
        fileOpsService.upload(client,"test.txt", inputStream);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN);
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> { fileOpsService.ls(client, "test.txt", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_PATTERN); });
    }
}
