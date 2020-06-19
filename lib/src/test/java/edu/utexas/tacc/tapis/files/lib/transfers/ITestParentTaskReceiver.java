package edu.utexas.tacc.tapis.files.lib.transfers;

import com.rabbitmq.client.ConnectionFactory;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.files.lib.utils.SystemsClientFactory;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestParentTaskReceiver {

    private TSystem sourceSystem;
    private TSystem destSystem;
    private Sender sender;
    private Receiver receiver;
    private IRemoteDataClientFactory remoteDataClientFactory;

    TenantManager tenantManager = Mockito.mock(TenantManager.class);
    SKClient skClient = Mockito.mock(SKClient .class);
    SystemsClient systemsClient = Mockito.mock(SystemsClient .class);
    ServiceJWT serviceJWT = Mockito.mock(ServiceJWT.class);

    @BeforeSuite
    private void doBeforeSuite() {
        //S3 system
        var creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        sourceSystem = new TSystem();
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
        sourceSystem = new TSystem();
        sourceSystem.setHost("http://localhost");
        sourceSystem.setBucketName("test");
        sourceSystem.setName("destSystem");
        sourceSystem.setPort(9000);
        sourceSystem.setAccessCredential(creds);
        sourceSystem.setRootDir("/");
        transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        sourceSystem.setTransferMethods(transferMechs);

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("dev");
        connectionFactory.setPassword("dev");
        connectionFactory.setVirtualHost("dev");
        connectionFactory.useNio();
        ReceiverOptions receiverOptions = new ReceiverOptions()
            .connectionFactory(connectionFactory)
            .connectionSubscriptionScheduler(Schedulers.newElastic("parent-receiver"));
        SenderOptions senderOptions = new SenderOptions()
            .connectionFactory(connectionFactory)
            .resourceManagementScheduler(Schedulers.newElastic("parent-sender"));
        this.receiver = RabbitFlux.createReceiver(receiverOptions);
        this.sender = RabbitFlux.createSender(senderOptions);

        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(1, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bind(RemoteDataClientFactory.class).to(IRemoteDataClient.class);
                bindAsContract(ParentTaskReceiver.class);
                bindAsContract(RemoteDataClientFactory.class);
                bindAsContract(TransfersService.class);
                bind(skClient).to(SKClient.class);
                bindAsContract(SystemsClientFactory.class);
                bindAsContract(ChildTaskReceiver.class);
                bindAsContract(FileTransfersDAO.class);
                bind(serviceJWT).to(ServiceJWT.class);
                bind(tenantManager).to(TenantManager.class);
            }
        });
        ParentTaskReceiver worker = locator.getService(ParentTaskReceiver.class);
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        Thread parentThread = new Thread(worker);
        parentThread.start();
    }

    @BeforeTest
    public void beforeTest() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("file1.txt", in);
        in = Utils.makeFakeFile(10*1024);
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
        when(systemsClient.getSystemByName(any(), any())).thenReturn(sourceSystem);
    }









}
