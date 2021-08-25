package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
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
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.test.StepVerifier;
import software.amazon.awssdk.annotations.SdkTestInternalApi;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    private String childQ;
    private String parentQ;


    @BeforeMethod
    public void setUpQueues() {
        this.childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(this.childQ);
        this.parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(this.parentQ);
    }

    @AfterMethod
    public void deleteQueues() {
        transfersService.deleteQueue(this.childQ).subscribe();
        transfersService.deleteQueue(this.parentQ).subscribe();
    }

    @BeforeMethod
    public void beforeMethod(Method method) {
        log.info("method name:" + method.getName());
    }


    @BeforeMethod
    public void initialize() throws Exception {
        Mockito.reset(skClient);
        Mockito.reset(serviceClients);
        Mockito.reset(systemsClient);
        Mockito.reset(permsService);
        sourceSystem = testSystemSSH;
        destSystem = testSystemS3;
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
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        // Task should be FAILED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.FAILED))
            .thenCancel()
            .verify(Duration.ofSeconds(5));
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
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        // Task should be STAGED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .thenCancel()
            .verify(Duration.ofSeconds(5));
        TransferTaskParent task = transfersService.getTransferTaskParentByUUID(t1.getParentTasks().get(0).getUuid());
        // The total size should be the sum of the 2 files inserted into the bucket in beforeTest()
        Assert.assertEquals(task.getTotalBytes(), 2 * 10 * 1024);

    }

    @Test
    public void testDoesListingAndCreatesChildTasks() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

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
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED))
            .thenCancel()
            .verify(Duration.ofSeconds(5));
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);

        // should be 2, one for each file created in the setUp method above;
        Assert.assertEquals(children.size(), 2);

    }

    @Test
    public void failsTransferWhenParentFails() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(null);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.FAILED))
            .thenCancel()
            .verify(Duration.ofSeconds(10));

        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.FAILED);
        Assert.assertNotNull(t1.getErrorMessage());
        Assert.assertNotNull(t1.getParentTasks().get(0).getErrorMessage());

        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 0);
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
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        StepVerifier
            .create(tasks)
            .assertNext(t -> {
                Assert.assertEquals(t.getStatus(), TransferTaskStatus.STAGED);
                Assert.assertEquals(t.getId(), t1.getId());
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 2);

    }

    @Test
    public void testEmptyDirectories() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        fileOpsService.insert(sourceClient,"a/1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.insert(sourceClient,"a/2.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.mkdir(sourceClient, "a/b/c/d/");

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a");
        element.setDestinationURI("tapis://destSystem/dest/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        //Take for 5 secs then finish
        stream.take(Duration.ofSeconds(5)).blockLast();

        List<FileInfo> listing = fileOpsService.ls(destClient, "/dest/b/c/");
        Assert.assertTrue(listing.size() > 0);
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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
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
    }

    /**
     * This test is important, basically testing a simple but complete transfer. We check the entries in the database
     * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
     * be fixed.
     * @throws Exception
     */
    @Test
    public void testNestedDirectories() throws Exception {
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
        fileOpsService.insert(sourceClient, "a/cat/dog/1.txt", in);
        in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "a/cat/dog/2.txt", in);

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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
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
        // 2 files, so total should be 2x
        Assert.assertEquals(parent.getBytesTransferred(), 2 * FILESIZE);

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b/cat/dog/");
        Assert.assertEquals(listing.size(), 2);
    }

    /**
     * Tests to se if the grouping does not hang after 256 groups
     */
    @Test(enabled=false)
    public void testMaxGroupSize() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(testSystemS3);
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


        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();

        for (var i=0; i<300; i++) {
            TransferTask t1 = transfersService.createTransfer(
                "testuser",
                "dev",
                "tag",
                elements
            );
        }
        AtomicInteger counter = new AtomicInteger();
        stream
            .subscribe( (t)->{
                counter.incrementAndGet();
                System.out.println(t.toString());
            }, (e)->{
                log.info(e.toString());
            });
        Thread.sleep(Duration.ofSeconds(60).toMillis());
        Assert.assertEquals(counter.get(), 300);
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

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("https://google.com");
        element.setDestinationURI("tapis://destSystem/b/input.html");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
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
        int FILESIZE = 100 * 1000 * 1024;
        InputStream in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "file1.txt", in);

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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
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
    }


    @Test(enabled = true)
    public void testDoesTransfersWhenOneErrors() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert(sourceClient,"a/1.txt", in);

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
            fileInfo.setType("file");
            TransferTaskChild child = new TransferTaskChild(parent, fileInfo);
            child = transfersService.createTransferTaskChild(child);
            transfersService.publishChildMessage(child);
            kids.add(child);
        }
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
            })
            .thenCancel()
            .verify(Duration.ofSeconds(10));

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        log.info(listing.toString());
        //NOT-THERE/1.txt should NOT BE THERE
        Assert.assertEquals(listing.size(), 1);

    }

    @Test()
    public void test100Files() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        for (var i=0;i<100;i++) {
            fileOpsService.insert(sourceClient, String.format("a/%s.txt", i), Utils.makeFakeFile(10000 * 1024));
        }
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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        stream.take(100).blockLast();

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 100);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }


    //TODO: I'm not sure why this test is failing? It has something to do with the clients
    @Test(enabled = false)
    public void testSameSystemForSourceAndDest() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(destSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        for (var i=0;i<2;i++) {
            fileOpsService.insert(sourceClient, String.format("a/%s.txt", i), Utils.makeFakeFile(10000 * 1024));
        }


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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        stream.take(Duration.ofSeconds(5)).blockLast();

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 2);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
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

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
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
                .verify(Duration.ofSeconds(60));

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 8);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }

    @Test
    public void testCancelSingleTransfer() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        int FILESIZE = 500 * 1000 * 1024;
        InputStream in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert(sourceClient, "file1.txt", in);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/file1.txt");
        element.setDestinationURI("tapis://destSystem/file1.txt");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );

        //Give it a sec to complete the parent task and get the child
        Thread.sleep(200);
        TransferTaskChild taskChild = transfersService.getAllChildrenTasks(t1).get(0);

        TransferTaskChild finalTaskChild = taskChild;
        Thread thread = new Thread( ()->{
            try {
                childTaskTransferService.doTransfer(finalTaskChild);
            } catch (Exception ignored){}
        });
        thread.start();
        Thread.sleep(2000);
        transfersService.cancelTransfer(t1);
        Thread.sleep(1000);
        taskChild = transfersService.getChildTaskByUUID(taskChild.getUuid());
        Assert.assertEquals(taskChild.getStatus(), TransferTaskStatus.CANCELLED);
        Assert.assertTrue(taskChild.getBytesTransferred() > 0);
    }

    @Test
    public void testCancelMultipleTransfers() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        int FILESIZE = 100 * 1000 * 1024;
        fileOpsService.insert(sourceClient, "file1.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "file2.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "file3.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "file4.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "file5.txt", Utils.makeFakeFile(FILESIZE));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();


        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            "tag",
            elements
        );
        StepVerifier.create(tasks)
            .assertNext((parent)-> {
                Assert.assertEquals(parent.getStatus(), TransferTaskStatus.STAGED);
            })
            .then(()-> {
                StepVerifier.create(childTaskTransferService.runPipeline())
                    .thenAwait(Duration.ofMillis(100))
                    .then(()->{
                        try {
                            transfersService.cancelTransfer(t1);
                        } catch (Exception ignored) {}
                    })
                    .assertNext((child)-> {
                        Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED);
                    })
                    .assertNext((child)-> {
                        Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED);
                    })
                    .assertNext((child)-> {
                        Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED);
                    })
                    .assertNext((child)-> {
                        Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED);
                    })
                    .assertNext((child)-> {
                        Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED);
                    })
                    .thenCancel()
                    .verify(Duration.ofSeconds(60));
            })
            .thenCancel()
            .verify(Duration.ofSeconds(60));
    }

    @Test
    public void testControlMessages() throws Exception {
        TransferControlAction action = new TransferControlAction();
        action.setTaskId(1);
        action.setTenantId("dev");
        action.setAction(TransferControlAction.ControlAction.CANCEL);

        transfersService.streamControlMessages()
            .take(Duration.ofSeconds(1))
            .subscribe((controlAction -> {
                Assert.assertEquals(controlAction.getTaskId(), 1);
            }));
        transfersService.publishControlMessage(action);
    }

    public void testFlux() {
        Flux<Integer> flux = Flux.just(1, 2, 3, null, 4, 5, 6)
            .flatMap(i-> Mono.just(i*2))
            .onErrorResume((e)->Flux.just(42));

        StepVerifier.create(flux)
            .expectNext(2)
            .expectNext(4)
            .expectNext(6)
            .expectNext(42)
            .expectComplete()
            .verify();
    }

}
