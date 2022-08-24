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
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
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

import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_LISTING_SIZE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Test the TransfersService.
 * NOTE: Currently no tests for multiple top level transfer request elements (i.e. parent tasks) in a transfer request.
 */
@Test(groups = {"integration"})
public class TestTransfers extends BaseDatabaseIntegrationTest
{
    private static final Logger log = LoggerFactory.getLogger(TestTransfers.class);

    private final String oboTenant = "oboTenant";
    private final String oboUser = "oboUser";
    private final String nullImpersonationId = null;
    private String childQ;
    private String parentQ;
    private ResourceRequestUser rTestUser =
        new ResourceRequestUser(new AuthenticatedUser("testuser", "dev", TapisThreadContext.AccountType.user.name(),
                                                      null, "testuser", "dev", null, null, null));


  public TestTransfers() throws Exception {
        super();
    }

    @BeforeMethod
    public void setUpQueues()
    {
        childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ).block(Duration.ofSeconds(1));
        parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ).block(Duration.ofSeconds(1));
    }

    @AfterMethod
    public void deleteQueues() {
        log.info("Deleting Queue: {}", childQ);
        transfersService.deleteQueue(childQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                (m)-> log.info("Deleted queue {}", childQ),
                (err)->log.error(err.getMessage(), err)
            );
        log.info("Deleting Queue: {}", parentQ);
        transfersService.deleteQueue(parentQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                (m)-> log.info("Deleted queue {}", parentQ),
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
    public void testNotPermitted(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
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
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        // Task should be FAILED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), TransferTaskStatus.FAILED))
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        // Parent task and top level TransferTask should have also failed
        TransferTaskParent task = transfersService.getTransferTaskParentByUUID(t1.getParentTasks().get(0).getUuid());
        Assert.assertEquals(task.getStatus(), TransferTaskStatus.FAILED);
        TransferTask topTask = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(topTask.getStatus(), TransferTaskStatus.FAILED);
        Mockito.reset(permsService);
    }

    // Tag is for notifications mainly
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
        TransferTask t1 = transfersService.createTransfer(rTestUser, "testTag", elements);

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
        fileOpsService.upload(sourceClient,"1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.upload(sourceClient,"2.txt", Utils.makeFakeFile(10 * 1024));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
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
    public void testDoesListingAndCreatesChildTasks(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);


        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        fileOpsService.upload(sourceClient,"1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.upload(sourceClient,"2.txt", Utils.makeFakeFile(10 * 1024));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
        TransferTask t2 = transfersService.createTransfer(rTestUser, "tag", elements);
        TransferTask t3 = transfersService.createTransfer(rTestUser, "tag", elements);
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
    public void failsTransferWhenParentFails(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        SystemsClient systemsClient = Mockito.mock(SystemsClient.class);
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"))).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"))).thenReturn(destSystem);
        when(serviceClients.getClient(anyString(), anyString(), eq(SystemsClient.class))).thenReturn(systemsClient);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/");
        element.setDestinationURI("tapis://destSystem/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

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
    public void testMultipleChildren(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        fileOpsService.upload(sourceClient,"a/1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.upload(sourceClient,"a/2.txt", Utils.makeFakeFile(10 * 1024));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a");
        element.setDestinationURI("tapis://destSystem/b");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
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

    // Test all system pairs except those involving S3 since S3 does not support directories
    @Test(dataProvider = "testSystemsDataProviderNoS3")
    public void testEmptyDirectories(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        fileOpsService.upload(sourceClient,"a/1.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.upload(sourceClient,"a/2.txt", Utils.makeFakeFile(10 * 1024));
        fileOpsService.mkdir(sourceClient, "a/b/c/d/");

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a");
        element.setDestinationURI("tapis://destSystem/dest/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        //Take for 5 secs then finish
        stream.take(Duration.ofSeconds(5)).blockLast();

        List<FileInfo> listing = fileOpsService.ls(destClient, "/dest/b/c/", MAX_LISTING_SIZE, 0);
        Assert.assertTrue(listing.size() > 0);
    }

    /*
     * This test is important, basically testing a simple but complete transfer. We check the entries in the database
     * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
     * be fixed.
     */
    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesTransfer(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
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
        fileOpsService.upload(sourceClient, "a/1.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.upload(sourceClient, "a/2.txt", Utils.makeFakeFile(FILESIZE));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
// StepVerify is part of reactor test framework (takes over stream and you can examine/assert it)
        StepVerifier
            .create(stream)
// Each item as it comes out of the stream should be in the completed state, have correct # bytes, etc.
// This is the 1st item
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
// This is the 2nd item
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED);
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
                Assert.assertNotNull(k.getStartTime());
                Assert.assertNotNull(k.getEndTime());
            })
// Only 2 files are transferred, so should be done
// Wrap things up
            .thenCancel()
// Give it up to 5 seconds to complete
            .verify(Duration.ofSeconds(5));

// Now we should be able to get the parent and child tasks and verify them
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

// Verify that transfers happened.
        List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 2);
    }

    @Test(dataProvider = "testSystemsDataProvider", enabled = false)
    public void testTransferExecutable(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
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
        fileOpsService.upload(sourceClient, "program.exe", Utils.makeFakeFile(FILESIZE));
        fileUtilsService.linuxOp(sourceClient, "/program.exe", FileUtilsService.NativeLinuxOperation.CHMOD, "755", false);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/program.exe");
        element.setDestinationURI("tapis://destSystem/program.exe");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

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

        List<FileInfo> listing = fileOpsService.ls(destClient, "program.exe", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 1);
        Assert.assertTrue(listing.get(0).getNativePermissions().contains("x"));
    }

  /*
   * This test is important, basically testing a simple but complete transfer. We check the entries in the database
   * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
   * be fixed.
   * NOTE: Test all system pairs except those involving S3 since S3 does not support directories
   */
  @Test(dataProvider = "testSystemsDataProviderNoS3")
  public void testNestedDirectories(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    log.info("srcSystem: {}", sourceSystem.getId());
    log.info("dstSystem: {}", destSystem.getId());
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
    fileOpsService.upload(sourceClient, "a/cat/dog/1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient, "a/cat/dog/2.txt", Utils.makeFakeFile(FILESIZE));

    TransferTaskRequestElement element = new TransferTaskRequestElement();
    element.setSourceURI("tapis://sourceSystem/a/");
    element.setDestinationURI("tapis://destSystem/b/");
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    elements.add(element);
    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    tasks.subscribe();
    Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
// Instead of using StepVerifier, block until everything is done.
// blockLast - start listening to stream and wait for it to finish.
// Had to do it this way because for S3 it is not known apriori how many items will come out of stream
    // 2 types of streams for reactor, cold and hot flux. hot - never-ending, null to hot flux will end it
    // these are all hot fluxes, next to lines set up an automatic send of a null to stop the hot stream after 5 seconds.
    stream.take(Duration.ofSeconds(5)).blockLast();

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

    List<FileInfo> listing = fileOpsService.ls(destClient, "/b/cat/dog/", MAX_LISTING_SIZE, 0);
    Assert.assertEquals(listing.size(), 2);
  }

  /**
     * Tests to se if the grouping does not hang after 256 groups
     * creates many threads, enable only as needed
     */
    @Test(dataProvider = "testSystemsDataProvider", enabled = false)
    public void testMaxGroupSize(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
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
        fileOpsService.upload(sourceClient, "a/1.txt", Utils.makeFakeFile(FILESIZE));


        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();
        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();

        for (var i=0; i<300; i++) { transfersService.createTransfer(rTestUser, "tag", elements); }
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
     */
    @Test(dataProvider = "testSystemsDataProvider")
    public void testHttpInputs(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
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
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

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

        List<FileInfo> listing = fileOpsService.ls(destClient,"/b", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 1);
    }


    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesTransferAtRoot(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
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
        fileOpsService.upload(sourceClient, "file1.txt", Utils.makeFakeFile(FILESIZE));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/file1.txt");
        element.setDestinationURI("tapis://destSystem/transferred");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

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

        List<FileInfo> listing = fileOpsService.ls(destClient, "/transferred", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 1);
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void testTransferSingleFile(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
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
        fileOpsService.upload(sourceClient, "file1.txt", in);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/file1.txt");
        element.setDestinationURI("tapis://destSystem/b/fileCopy.txt");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

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

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 1);
    }

  /**
   * Test for optional parentTask - transfer should not fail if the parentTask fails
   * Note that this test involves a special case where the parent has no children because
   *   the single element in the txfr request is for a path that does not exist.
   *   So when ParentTaskTransferService attempts to list the path in order to create child tasks it throws an error
   *   and the txfr fails without creating any child tasks.
   * NOTE: Final update fo TransferTask happens in ChildTaskTransferService stepFour() or doErrorStepOne()
   *       or in ParentTaskTransferService doErrorParentStepOne
   * NOTE: Leave out S3 system just to save time.
   * @param systemsPair system pairs for testing
   * @throws Exception on error
   */
    @Test(dataProvider = "testSystemsDataProviderNoS3", enabled = true)
    public void testTransferOptionalNoFail(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
      TapisSystem sourceSystem = systemsPair.getLeft();
      TapisSystem destSystem = systemsPair.getRight();
      log.info("srcSystem: {}", sourceSystem.getId());
      log.info("dstSystem: {}", destSystem.getId());
      when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
      when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
      when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

      IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
      IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
      //wipe out the dest folder just in case
      fileOpsService.delete(destClient, "/");

      // Add an option transfer request element that should fail because source file does not exist
      List<TransferTaskRequestElement> elements = new ArrayList<>();
      TransferTaskRequestElement element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/1.txt");
      element.setDestinationURI("tapis://destSystem/b/1.txt");
      element.setOptional(true);
      elements.add(element);
      TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
      Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
      tasks.subscribe();
      Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
      // Give it time to finish then check the task status
      stream.take(Duration.ofSeconds(5)).blockLast();
      // Now we should be able to get the transfer task and it should not have failed
      t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
      Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }

    // Make sure that one error does not impact other transfers.
    @Test(dataProvider = "testSystemsDataProvider")
    public void testDoesTransfersWhenOneFails(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.upload(sourceClient,"a/1.txt", in);

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
        TransferTaskParent parent = t1.getParentTasks().get(0);
        List<TransferTaskChild> kids = new ArrayList<>();
        for (String path : new String[]{"/NOT-THERE/1.txt", "/a/2.txt"})
        {
            FileInfo fileInfo = new FileInfo();
            fileInfo.setPath(path);
            fileInfo.setSize(10 * 1024);
            fileInfo.setType(FileInfo.FILETYPE_FILE);
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
            .assertNext(k->{ Assert.assertEquals(k.getStatus(), TransferTaskStatus.FAILED); })
            .assertNext(k->{ Assert.assertEquals(k.getStatus(), TransferTaskStatus.FAILED); })
            .thenCancel()
            .verify(Duration.ofSeconds(10));
    }

    @Test(dataProvider = "testSystemsDataProvider")
    public void test10Files(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add 10 files to transfer
        for (var i=0; i<10; i++)
        {
          fileOpsService.upload(sourceClient, String.format("a/%s.txt", i), Utils.makeFakeFile(10000 * 1024));
        }
        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        stream.take(Duration.ofSeconds(5)).blockLast();

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 10);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }

    @Test(dataProvider = "testSystemsListDataProvider", enabled = true)
    public void testSameSystemForSourceAndDest(TapisSystem testSystem) throws Exception {
        TapisSystem sourceSystem = testSystem;
        TapisSystem destSystem = testSystem;
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //Add some files to transfer
        fileOpsService.upload(sourceClient, "a/1.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient, "a/2.txt", Utils.makeFakeFile(10000 * 1024));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        StepVerifier
            .create(stream)
            .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
            .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
            .thenCancel()
            .verify(Duration.ofSeconds(5));

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 2);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }


    // This test follows same pattern as testDoesTransfer
    @Test(dataProvider = "testSystemsDataProvider")
    public void testFullPipeline(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");
        //Add some files to transfer
        fileOpsService.upload(sourceClient,"/a/1.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/2.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/3.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/4.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/5.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/6.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/7.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.upload(sourceClient,"/a/8.txt", Utils.makeFakeFile(10000 * 1024));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
        tasks.subscribe();

        Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
        StepVerifier
                .create(stream)
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
                .thenCancel()
                .verify(Duration.ofSeconds(60));

        List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0);
        Assert.assertEquals(listing.size(), 8);
        t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    }

    // Difficult to test cancel, need for txfrs to have started but not finished before attempting to cancel
    // Tricky timing
    @Test(dataProvider = "testSystemsDataProvider")
    public void testCancelMultipleTransfers(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
    {
        TapisSystem sourceSystem = systemsPair.getLeft();
        TapisSystem destSystem = systemsPair.getRight();
        log.info("srcSystem: {}", sourceSystem.getId());
        log.info("dstSystem: {}", destSystem.getId());
        when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
        when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, destSystem, "testuser");

        //wipe out the dest folder just in case
        fileOpsService.delete(destClient, "/");

        //Add some files to transfer
        int FILESIZE = 100 * 1000 * 1024;
        fileOpsService.upload(sourceClient, "a/file1.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.upload(sourceClient, "a/file2.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.upload(sourceClient, "a/file3.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.upload(sourceClient, "a/file4.txt", Utils.makeFakeFile(FILESIZE));
        fileOpsService.upload(sourceClient, "a/file5.txt", Utils.makeFakeFile(FILESIZE));

        TransferTaskRequestElement element = new TransferTaskRequestElement();
        element.setSourceURI("tapis://sourceSystem/a/");
        element.setDestinationURI("tapis://destSystem/b/");
        List<TransferTaskRequestElement> elements = new ArrayList<>();
        elements.add(element);
        Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();

        TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
        StepVerifier.create(tasks)
            .assertNext((parent)-> { Assert.assertEquals(parent.getStatus(), TransferTaskStatus.STAGED); })
            .then(()-> {
              StepVerifier
                      .create(childTaskTransferService.runPipeline())
                      .thenAwait(Duration.ofMillis(100))
                      .then(() -> { try { transfersService.cancelTransfer(t1); } catch (Exception ignored) {} })
                      .assertNext((child)-> { Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED); })
                      .assertNext((child)-> { Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED); })
                      .assertNext((child)-> { Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED); })
                      .assertNext((child)-> { Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED); })
                      .assertNext((child)-> { Assert.assertEquals(child.getStatus(), TransferTaskStatus.CANCELLED); })
                      .thenCancel()
                      .verify(Duration.ofSeconds(5));
            })
            .thenCancel()
            .verify(Duration.ofSeconds(5));
    }

    // Make sure we can pick up control messages, such as the cancel control msg
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

    // Test of general flux support to see how it works, especially error handling
   // can run these tests with breakpoints to see what is going on.
   // Cold flux - to stop, either reaches end of items in the stream or encounters a null
    public void testFlux() {
        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5, 6)
            .flatMap(i->((i % 2)==0) ? Mono.just(i / 0): Mono.just(i))
            .onErrorContinue((e,o)-> { log.info(e.getMessage(), o); })
            .flatMap(i -> Mono.just(i*i))
            .doOnNext(i->log.info(i.toString()));

        StepVerifier.create(flux)
            .expectNext(1).expectNext(9).expectNext(25)
            .expectComplete().verify();
    }

    // Investigate onErrorResume
    // On even numbers throw error, should return 1 and keep going, i.e. on error provide fallback value
    @Test(enabled = false)
    public void testFlux2()
    {
        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5, 6)
            .flatMap(i->((i % 2)==0) ? Mono.just(i / 0): Mono.just(i))
            .onErrorResume((e)-> { log.info(e.getMessage()); return Mono.just(1); })
            .flatMap(i -> Mono.just(i*i))
            .doOnNext(i->log.info(i.toString()));

        StepVerifier.create(flux)
            .expectNext(1).expectNext(1).expectNext(9).expectNext(1).expectNext(25).expectNext(1)
            .expectComplete().verify();
    }
}
