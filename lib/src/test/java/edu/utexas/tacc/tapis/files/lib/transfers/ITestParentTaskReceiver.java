package edu.utexas.tacc.tapis.files.lib.transfers;

import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.ITransfersService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;
import reactor.test.StepVerifier;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestParentTaskReceiver {

    private TSystem sourceSystem;
    private TSystem destSystem;
    private IRemoteDataClientFactory remoteDataClientFactory;

    TenantManager tenantManager = Mockito.mock(TenantManager.class);
    SKClient skClient = Mockito.mock(SKClient.class);
    SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
    SystemsClientFactory systemsClientFactory = Mockito.mock(SystemsClientFactory.class);
    ServiceJWT serviceJWT;
    TransfersService transfersService;

    @BeforeSuite
    private void doBeforeSuite() throws Exception {
        //S3 system
        var creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        sourceSystem = new TSystem();
        sourceSystem.setTenant("dev");
        sourceSystem.setHost("http://localhost");
        sourceSystem.setBucketName("test");
        sourceSystem.setName("sourceSystem");
        sourceSystem.setPort(9000);
        sourceSystem.setAccessCredential(creds);
        sourceSystem.setRootDir("/");
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        sourceSystem.setTransferMethods(transferMechs);

        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        destSystem = new TSystem();
        destSystem.setTenant("dev");
        destSystem.setHost("http://localhost");
        destSystem.setBucketName("test");
        destSystem.setName("destSystem");
        destSystem.setPort(9000);
        destSystem.setAccessCredential(creds);
        destSystem.setRootDir("/");
        transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        destSystem.setTransferMethods(transferMechs);

        var tenant = new Tenant();
        tenant.setTenantId("testTenant");
        tenant.setBaseUrl("https://test.tapis.io");
        Map<String, Tenant> tenantMap = new HashMap<>();
        tenantMap.put(tenant.getTenantId(), tenant);
        when(tenantManager.getTenants()).thenReturn(tenantMap);
        serviceJWT = Mockito.mock(ServiceJWT.class);
        var serviceJWTFactory = Mockito.mock(ServiceJWTCacheFactory.class);
        when(serviceJWTFactory.provide()).thenReturn(serviceJWT);
        when(systemsClientFactory.getClient(any(), any())).thenReturn(systemsClient);

//        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();

        ServiceLocator locator = ServiceLocatorUtilities.bind(new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(ParentTaskFSM.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(ParentTaskReceiver.class);
                bindAsContract(TransfersService.class);
                bindAsContract(RemoteDataClientFactory.class);
                bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);

                bind(systemsClientFactory).to(SystemsClientFactory.class);
                bind(systemsClient).to(SystemsClient.class);
                bind(tenantManager).to(TenantManager.class);
                bind(skClient).to(SKClient.class);
                bind(serviceJWTFactory).to(ServiceJWTCacheFactory.class);
                bind(serviceJWT).to(ServiceJWT.class);
                bind(tenantManager).to(TenantManager.class);
            }
        });
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        transfersService = locator.getService(TransfersService.class);

    }

    @BeforeTest
    public void beforeTest() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("file1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("file2.txt", in);
    }

    @AfterTest()
    public void tearDown() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        fileOpsService.delete("/");
        client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        fileOpsService = new FileOpsService(client);
        fileOpsService.delete("/");
    }

    @Test
    public void testDoesListing() throws Exception {
        when(systemsClient.getSystemByName(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemByName(eq("destSystem"), any())).thenReturn(destSystem);

        TransferTask task = new TransferTask();
        task.setTenantId("test");
        task.setUsername("test");
        transfersService.createTransfer("testUser1", "dev",
            sourceSystem.getName(),
            "/file1.txt",
            destSystem.getName(),
            "/"
        );

    }

    @Test
    public void testDoes() throws Exception {
        when(systemsClient.getSystemByName(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemByName(eq("destSystem"), any())).thenReturn(destSystem);
        TransferTask t1 = transfersService.createTransfer("testUser1", "dev",
            sourceSystem.getName(),
            "/file1.txt",
            destSystem.getName(),
            "/"
        );
        TransferTask t2 = transfersService.createTransfer("testUser1", "dev2",
            sourceSystem.getName(),
            "/file1.txt",
            destSystem.getName(),
            "/"
        );
        TransferTask t3 = transfersService.createTransfer("testUser1", "dev3",
            sourceSystem.getName(),
            "/file1.txt",
            destSystem.getName(),
            "/"
        );
        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTask> tasks = transfersService.processParentTasks(messages);
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), "STAGED"))
            .assertNext(t -> Assert.assertEquals(t.getStatus(), "STAGED"))
            .assertNext(t -> Assert.assertEquals(t.getStatus(), "STAGED"))
            .thenCancel()
            .verify();
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 1);
    }

    @Test
    public void testMultipleChildren() throws Exception {
        when(systemsClient.getSystemByName(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemByName(eq("destSystem"), any())).thenReturn(destSystem);

        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/2.txt", in);
        TransferTask t1 = transfersService.createTransfer("testUser1", "dev",
            sourceSystem.getName(),
            "/a",
            destSystem.getName(),
            "/a"
        );

        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTask> tasks = transfersService.processParentTasks(messages);
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), "STAGED"))
            .thenCancel()
            .verify();
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 2);
    }
}
