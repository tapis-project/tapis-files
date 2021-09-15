package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferControlAction;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskRequestElement;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
    private String childQ;
    private String parentQ;

    public ITestTransfers() throws Exception {
        super();
    }

    @BeforeMethod
    public void setUpQueues() {
        this.childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(this.childQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                (m)-> log.info(m.toString()),
                (err)->log.error(err.getMessage(), err)
            );
        this.parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(this.parentQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                (m)-> log.info(m.toString()),
                (err)-> log.error(err.getMessage(), err)
            );
    }

    @AfterMethod
    public void deleteQueues() {
        log.info("Deleting Queue: {}", this.childQ);
        transfersService.deleteQueue(this.childQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                (m)-> log.info("Deleted queue {}", this.childQ),
                (err)->log.error(err.getMessage(), err)
            );
        log.info("Deleting Queue: {}", this.parentQ);
        transfersService.deleteQueue(this.parentQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                (m)-> log.info("Deleted queue {}", this.parentQ),
                (err)-> log.error(err.getMessage(), err)
            );
    }

    @BeforeMethod
    public void beforeMethod(Method method) {
        log.info("method name:" + method.getName());
    }


    @AfterMethod
    public void initialize() {
        Mockito.reset(systemsCache);
        Mockito.reset(serviceClients);
        Mockito.reset(permsService);
    }

    @AfterMethod
    @BeforeMethod
    public void tearDown() throws Exception {
        Mockito.reset(permsService);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        for (TapisSystem system: testSystems) {
            IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, system, "testuser");
            fileOpsService.delete(client,"/");
        }
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testNotPermitted(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
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
        Flux
            <TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        // Task should be FAILED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.FAILED))
            .thenCancel()
            .verify(Duration.ofSeconds(5));
        TransferTaskParent task = transfersService.getTransferTaskParentByUUID(t1.getParentTasks().get(0).getUuid());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.FAILED);

        TransferTask topTask = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(topTask.getStatus(), TransferTaskStatus.FAILED);

        Mockito.reset(permsService);

    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testTagSaveAndReturned(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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


    @Test(dataProvider = "testSystemsDataProvider")
    public void testUpdatesTransferSize(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        fileOpsService.insert(sourceClient,"1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.insert(sourceClient,"2.txt", Utils.makeFakeFile(10 * 1024));

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

    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesListingAndCreatesChildTasks(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);


        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        fileOpsService.insert(sourceClient,"1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.insert(sourceClient,"2.txt", Utils.makeFakeFile(10 * 1024));

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

    @Test(dataProvider = "testSystemsDataProvider")
    public void failsTransferWhenParentFails(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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



    @Test(dataProvider = "testSystemsDataProvider")
    public void testMultipleChildren(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {

        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        fileOpsService.insert(sourceClient,"a/1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.insert(sourceClient,"a/2.txt", Utils.makeFakeFile(10 * 1024));

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
            .verify(Duration.ofSeconds(600));
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 2);
        Mockito.reset(systemsCache);

    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testEmptyDirectories(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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

    /**
     * This test is important, basically testing a simple but complete transfer. We check the entries in the database
     * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
     * be fixed.
     * @throws Exception
     */
    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesTransfer(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1024;
        fileOpsService.insert(sourceClient, "a/1.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "a/2.txt", Utils.makeFakeFile(FILESIZE));

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

    @Test(dataProvider = "testSystemsDataProvider", enabled = false)
    public void testTransferExecutable(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");
        fileOpsService.delete(sourceClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        fileOpsService.insert(sourceClient, "program.exe", Utils.makeFakeFile(FILESIZE));
        fileUtilsService.linuxOp(sourceClient, "/program.exe", FileUtilsService.NativeLinuxOperation.CHMOD, "755", false);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/program.exe");
        element.setDestinationURI("tapis://destSystem/program.exe");
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
            .thenCancel()
            .verify(Duration.ofSeconds(600));

        List<FileInfo> listing = fileOpsService.ls(destClient, "program.exe");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertTrue(listing.get(0).getNativePermissions().contains("x"));
    }


    /**
     * This test is important, basically testing a simple but complete transfer. We check the entries in the database
     * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
     * be fixed.
     * @throws Exception
     */
    @Test(dataProvider = "testSystemsDataProvider")
    public void testNestedDirectories(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        fileOpsService.insert(sourceClient, "a/cat/dog/1.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "a/cat/dog/2.txt", Utils.makeFakeFile(FILESIZE));

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
        stream.take(Duration.ofSeconds(5))
            .blockLast();

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
    @Test(dataProvider = "testSystemsDataProvider", enabled = false)
    public void testMaxGroupSize(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        fileOpsService.insert(sourceClient, "a/1.txt", Utils.makeFakeFile(FILESIZE));


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
    @Test(dataProvider = "testSystemsDataProvider")
    public void testHttpInputs(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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


    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesTransferAtRoot(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        // Double check that the files really are in the destination
        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");


        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        fileOpsService.insert(sourceClient, "file1.txt", Utils.makeFakeFile(FILESIZE));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/file1.txt");
        element.setDestinationURI("tapis://destSystem/transferred");
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

        List<FileInfo> listing = fileOpsService.ls(destClient, "/transferred");
        Assert.assertEquals(listing.size(), 1);
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testTransferSingleFile(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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
        element.setDestinationURI("tapis://destSystem/b/fileCopy.txt");
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
            .verify(Duration.ofSeconds(100));

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
        Assert.assertEquals(listing.size(), 1);
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesTransfersWhenOneErrors(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.FAILED);
            })
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.FAILED);
            })
            .thenCancel()
            .verify(Duration.ofSeconds(10));

    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void test10Files(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);


        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        for (var i=0;i<10;i++) {
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
        Assert.assertEquals(listing.size(), 10);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }


    @Test(dataProvider = "testSystemsListDataProvider", enabled = true)
    public void testSameSystemForSourceAndDest(TapisSystem testSystem) throws Exception {
        TapisSystem sourceSystem = testSystem;
        TapisSystem destSystem = testSystem;
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer

        fileOpsService.insert(sourceClient, "a/1.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert(sourceClient, "a/2.txt", Utils.makeFakeFile(10000 * 1024));


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
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b");
        Assert.assertEquals(listing.size(), 2);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testFullPipeline(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

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

    @Test(dataProvider = "testSystemsDataProvider")
    public void testCancelMultipleTransfers(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info(sourceSystem.getId());
        log.info(destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        int FILESIZE = 100 * 1000 * 1024;
        fileOpsService.insert(sourceClient, "a/file1.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "a/file2.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "a/file3.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "a/file4.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.insert(sourceClient, "a/file5.txt", Utils.makeFakeFile(FILESIZE));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
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
                    .verify(Duration.ofSeconds(5));
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));
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
        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5, 6)
            .flatMap(i->((i % 2)==0) ? Mono.just(i / 0): Mono.just(i))
            .onErrorContinue((e,o)-> {
                log.info(e.getMessage(), o);
            })
            .flatMap(i -> Mono.just(i*i))
            .doOnNext(i->log.info(i.toString()));

        StepVerifier.create(flux)
            .expectNext(1)
            .expectNext(9)
            .expectNext(25)
            .expectComplete()
            .verify();
    }

    @Test(enabled = false)
    public void testFlux2() {
        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5, 6)
            .flatMap(i->((i % 2)==0) ? Mono.just(i / 0): Mono.just(i))
            .onErrorResume((e)-> {
                log.info(e.getMessage());
                return Mono.just(1);
            })
            .flatMap(i -> Mono.just(i*i))
            .doOnNext(i->log.info(i.toString()));

        StepVerifier.create(flux)
            .expectNext(1)
            .expectNext(1)
            .expectNext(9)
            .expectNext(1)
            .expectNext(25)
            .expectNext(1)
            .expectComplete()
            .verify();
    }

}
