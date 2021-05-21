package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.test.StepVerifier;
import software.amazon.awssdk.annotations.SdkTestInternalApi;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestTransfers extends BaseDatabaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ITestTransfers.class);

    private final String oboTenant = "oboTenant";
    private final String oboUser = "oboUser";
    private TapisSystem sourceSystem;
    private TapisSystem destSystem;

    @BeforeMethod
    public void initialize() throws Exception {
        Mockito.reset(skClient);
        Mockito.reset(serviceClients);
        Mockito.reset(systemsClient);
        Mockito.reset(permsService);
        sourceSystem = testSystemS3;
        destSystem = testSystemSSH;
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10 * 1024);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        fileOpsService.insert(client,"/file1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert(client,"/file2.txt", in);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Mockito.reset(skClient);
        Mockito.reset(permsService);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        fileOpsService.delete(client,"/");
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        fileOpsService.delete(client,"/");
    }

    @Test
    public void testNotPermitted() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTaskParent> tasks = transfersService.processParentTasks(messages);
        // Task should be FAILED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.FAILED))
            .thenCancel()
            .verify();
        TransferTaskParent task = transfersService.getTransferTaskParentByUUID(t1.getParentTasks().get(0).getUuid());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.FAILED);

        TransferTask topTask = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.FAILED);

        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();
        Mockito.reset(permsService);

    }


    @Test
    public void testTagSaveAndReturned() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "testTag",
            elements
        );

        Assert.assertEquals(t1.getTag(), "testTag");
    }


    @Test
    public void testUpdatesTransferSize() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTaskParent> tasks = transfersService.processParentTasks(messages);
        // Task should be STAGED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .thenCancel()
            .verify();
        TransferTaskParent task = transfersService.getTransferTaskParentByUUID(t1.getParentTasks().get(0).getUuid());
        // The total size should be the sum of the 2 files inserted into the bucket in beforeTest()
        Assert.assertEquals(task.getTotalBytes(), 2 * 10 * 1024);

        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();

    }

    @Test
    public void testDoesListingAndCreatesChildTasks() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        TransferTask t2 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        TransferTask t3 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTaskParent> tasks = transfersService.processParentTasks(messages);
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .thenCancel()
            .verify();
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);

        // should be 2, one for each file created in the setUp method above;
        Assert.assertEquals(children.size(), 2);

        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }

    @Test
    public void failsTransferWhenParentFails() throws Exception {
        //TODO: this is a cheesy way to make this break, but it does.
        sourceSystem.setHost("");
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTaskParent> tasks = transfersService.processParentTasks(messages);
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.FAILED))
            .thenCancel()
            .verify(Duration.ofSeconds(60));

        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.FAILED);
        Assert.assertNotNull(t1.getErrorMessage());
        Assert.assertNotNull(t1.getParentTasks().get(0).getErrorMessage());

        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);

        // should be 2, one for each file created in the setUp method above;
        Assert.assertEquals(children.size(), 0);

        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
        sourceSystem.setHost("http://localhost");
    }



    @Test
    public void testMultipleChildren() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert(sourceClient,"a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert(sourceClient,"a/2.txt", in);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a");
        element.setDestinationURI("tapis://destSystem/b");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTaskParent> tasks = transfersService.processParentTasks(messages);
        StepVerifier
            .create(tasks)
            .assertNext(t -> {
                Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED);
                Assert.assertEquals(t.getId(), t1.getId());
            })
            .thenCancel()
            .verify();
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 2);

        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();


    }


    @DataProvider
    private Object[] testSourcesProvider() {
        return new String[] {"tapis://sourceSystem/a/", "https://google.com"};
    }

    /**
     * This test is important, basically testing a simple but complete transfer. We check the entries in the database
     * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
     * be fixed.
     * @throws Exception
     */
    @Test
    public void testDoesTransfer() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        InputStream in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "a/1.txt", in);
        in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "a/2.txt", in);

        //Fix the queues to something random to avoid any lingering messages
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> parentTaskStream = transfersService.streamParentMessages();
        transfersService.processParentTasks(parentTaskStream).subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(t1.getStartTime());
        Assert.assertNotNull(t1.getEndTime());

        //Check for parent Task properties too
        TransferTaskParent parent = t1.getParentTasks().get(0);
        Assert.assertEquals(parent.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(parent.getEndTime());
        Assert.assertNotNull(parent.getStartTime());
        Assert.assertTrue(parent.getBytesTransferred() > 0);

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 2);
        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();
    }


    /**
     * This test is important, basically testing a simple but complete transfer. We check the entries in the database
     * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
     * be fixed.
     * @throws Exception
     */
    @Test
    public void testHttpInputs() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Fix the queues to something random to avoid any lingering messages
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("https://google.com");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> parentTaskStream = transfersService.streamParentMessages();
        transfersService.processParentTasks(parentTaskStream).subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertTrue(k.getBytesTransferred() > 0);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(t1.getStartTime());
        Assert.assertNotNull(t1.getEndTime());

        //Check for parent Task properties too
        TransferTaskParent parent = t1.getParentTasks().get(0);
        Assert.assertEquals(parent.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(parent.getEndTime());
        Assert.assertNotNull(parent.getStartTime());
        Assert.assertTrue(parent.getBytesTransferred() > 0);

        List<FileInfo> listing = fileOpsService.ls(destClient,"/b");
        Assert.assertEquals(listing.size(), 1);

        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();
    }


    @Test
    public void testDoesTransferAtRoot() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        InputStream in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "file1.txt", in);

        //Fix the queues to something random to avoid any lingering messages
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/file1.txt");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> parentTaskStream = transfersService.streamParentMessages();
        transfersService.processParentTasks(parentTaskStream).subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(t1.getStartTime());
        Assert.assertNotNull(t1.getEndTime());

        //Check for parent Task properties too
        TransferTaskParent parent = t1.getParentTasks().get(0);
        Assert.assertEquals(parent.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(parent.getEndTime());
        Assert.assertNotNull(parent.getStartTime());
        Assert.assertTrue(parent.getBytesTransferred() > 0);

        List<FileInfo> listing = fileOpsService.ls(destClient, "/");
        Assert.assertEquals(listing.size(), 1);
        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();
    }

    @Test
    public void testTransferSingleFile() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        InputStream in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "file1.txt", in);

        //Fix the queues to something random to avoid any lingering messages
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/file1.txt");
        element.setDestinationURI("tapis://destSystem/file1.txt");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> parentTaskStream = transfersService.streamParentMessages();
        transfersService.processParentTasks(parentTaskStream).subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(t1.getStartTime());
        Assert.assertNotNull(t1.getEndTime());

        //Check for parent Task properties too
        TransferTaskParent parent = t1.getParentTasks().get(0);
        Assert.assertEquals(parent.getStatus(), TransferTaskStatus.COMPLETED);
        Assert.assertNotNull(parent.getEndTime());
        Assert.assertNotNull(parent.getStartTime());
        Assert.assertTrue(parent.getBytesTransferred() > 0);

        List<FileInfo> listing = fileOpsService.ls(destClient, "file1.txt");
        Assert.assertEquals(listing.size(), 1);
        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();
    }


    @Test(enabled = true)
    public void testDoesTransfersWhenOneErrors() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert(sourceClient,"a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert(sourceClient, "a/2.txt", in);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        TransferTaskParent parent = t1.getParentTasks().get(0);
        List<TransferTaskChild> kids = new ArrayList<>();
        for(String path : new String[]{"/NOT-THERE/1.txt", "/a/2.txt"}){
            FileInfo fileInfo = new FileInfo();
            fileInfo.setPath(path);
            fileInfo.setSize(10 * 1024);
            TransferTaskChild child = new TransferTaskChild(parent, fileInfo);
            child = transfersService.createTransferTaskChild(child);
            transfersService.publishChildMessage(child);
            kids.add(child);
        }
        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
            })
            .thenCancel()
            .verify(Duration.ofSeconds(10));

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        //NOT-THERE/1.txt should NOT BE THERE
        Assert.assertEquals(listing.size(), 1);
        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();

    }

    @Test()
    public void testFullPipeline() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        fileOpsService.insert(sourceClient,"/a/1.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/2.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/3.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/4.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/5.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/6.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/7.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient,"/a/8.txt", Utils.makeFakeFile(10000 * 1024));


        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTaskParent> parentStream = transfersService.processParentTasks(parentMessageStream);
        parentStream.subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
                .create(stream)
                 .assertNext(t-> {
                     Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                 })
                 .assertNext(t->{
                     Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                 })
                .assertNext(t->{
                    Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                })
                .assertNext(t->{
                    Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                })
                .assertNext(t-> {
                    Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                })
                .assertNext(t-> {
                     Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                })
                .assertNext(t-> {
                    Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                })
                .assertNext(t-> {
                    Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
                })
                .thenCancel()
                .verify(Duration.ofSeconds(30));

        stream.subscribe(taskChild -> log.info(taskChild.toString()));
        // MUST sleep here for a bit for a bit for things to resolve. Alternatively could
        // use a StepVerifier or put some of this in the subscribe callback
        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 8);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }

    @Test(groups={"performance"})
    public void testConcurrency() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        fileOpsService.insert(sourceClient,"/a/1.txt", Utils.makeFakeFile(1000000 * 1024));
        fileOpsService.insert(sourceClient,"/a/2.txt", Utils.makeFakeFile(1000000 * 1024));
        fileOpsService.insert(sourceClient,"/a/3.txt", Utils.makeFakeFile(1000000 * 1024));


        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTaskParent> parentStream = transfersService.processParentTasks(parentMessageStream);
        parentStream.subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(t-> {
                Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
            })
            .assertNext(t->{
                Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
            })
            .assertNext(t->{
                Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED);
            })
            .thenCancel()
            .verify(Duration.ofSeconds(30));

        stream.subscribe(taskChild -> log.info(taskChild.toString()));
        // MUST sleep here for a bit for a bit for things to resolve. Alternatively could
        // use a StepVerifier or put some of this in the subscribe callback
        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 3);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }

}
