package edu.utexas.tacc.tapis.files.lib.transfers;

import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.S3DataClient;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestParentTaskReceiver {

    private static final Logger log = LoggerFactory.getLogger(ITransfersService.class);
    private TSystem sourceSystem;
    private TSystem destSystem;
    private IRemoteDataClientFactory remoteDataClientFactory;
    private ServiceLocator locator;

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
        sourceSystem.setBucketName("test1");
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
        destSystem.setBucketName("test2");
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

        locator = ServiceLocatorUtilities.bind(new AbstractBinder() {
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

    @BeforeMethod
    public void beforeTest() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        try {
            client.makeBucket("test1");
            client.makeBucket("test2");
        } catch (Exception ex) {

        }
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("file1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("file2.txt", in);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        fileOpsService.delete("/");
        client = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
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
    public void testDoesListingAndCreatesChildTasks() throws Exception {
        when(systemsClient.getSystemByName(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemByName(eq("destSystem"), any())).thenReturn(destSystem);
        String qname = UUID.randomUUID().toString();
        transfersService.setParentQueue(qname);
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
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(sourceClient);
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/2.txt", in);
        String qname = UUID.randomUUID().toString();
        transfersService.setParentQueue(qname);
        TransferTask t1 = transfersService.createTransfer("testUser1", "dev",
            sourceSystem.getName(),
            "/a/",
            destSystem.getName(),
            "/b/"
        );

        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTask> tasks = transfersService.processParentTasks(messages);
        StepVerifier
            .create(tasks)
            .assertNext(t -> {
                Assert.assertEquals(t.getStatus(), "STAGED");
                Assert.assertEquals(t.getId(), t1.getId());
            })
            .then(()->transfersService.deleteQueue(qname))
            .thenCancel()
            .verify();
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 2);
    }

    @Test
    public void testDoesTransfer() throws Exception {
        when(systemsClient.getSystemByName(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemByName(eq("destSystem"), any())).thenReturn(destSystem);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(sourceClient);
        //Add some files to transfer
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/2.txt", in);
        String qname = UUID.randomUUID().toString();
        transfersService.setParentQueue(qname);

        TransferTask t1 = transfersService.createTransfer(
            "testUser1",
            "dev",
            sourceSystem.getName(),
            "/a/",
            destSystem.getName(),
            "/b/"
        );
        FileInfo fileInfo = new FileInfo();
        fileInfo.setPath("/a/1.txt");
        fileInfo.setSize(10*1024);
        TransferTaskChild child = new TransferTaskChild(t1, fileInfo);
        child = transfersService.createTransferTaskChild(child);
        TransferTaskChild task = transfersService.doTransfer(child);
        log.info(task.toString());
        IFileOpsService fileOpsServiceDestination = new FileOpsService(destClient);
        List<FileInfo> listing = fileOpsServiceDestination.ls("/b");
        Assert.assertEquals(listing.size(), 1);


    }
}
