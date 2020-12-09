package edu.utexas.tacc.tapis.files.lib.transfers;

import edu.utexas.tacc.tapis.files.lib.BaseDatabaseIntegrationTest;
import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.test.StepVerifier;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Test(groups = {"integration"})
public class ITestTransfers extends BaseDatabaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ITestTransfers.class);

    private TSystem sourceSystem;
    private TSystem destSystem;

    @BeforeMethod
    public void beforeTest() throws Exception {
        sourceSystem = testSystemS3;
        destSystem = testSystemSSH;
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
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
    public void testUpdatesTransferSize() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTask t1 = transfersService.createTransfer("testuser", "dev",
            sourceSystem.getName(),
            "/",
            destSystem.getName(),
            "/"
        );
        Flux<AcknowledgableDelivery> messages = transfersService.streamParentMessages();
        Flux<TransferTask> tasks = transfersService.processParentTasks(messages);
        // Task should be STAGED after the pipeline runs
        StepVerifier
            .create(tasks)
            .assertNext(t -> Assert.assertEquals(t.getStatus(), "STAGED"))
            .thenCancel()
            .verify();
        TransferTask task = transfersService.getTransferTaskByUUID(t1.getUuid());
        // The total size should be the sum of the 2 files inserted into the bucket in beforeTest()
        Assert.assertEquals(task.getTotalBytes(), 2 * 10 * 1024);

        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();

    }

    @Test
    public void testDoesListingAndCreatesChildTasks() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);
        TransferTask t1 = transfersService.createTransfer("testuser", "dev",
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

        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }

    @Test
    public void testMultipleChildren() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(sourceClient);
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/2.txt", in);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTask t1 = transfersService.createTransfer("testuser", "dev",
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
            .thenCancel()
            .verify();
        List<TransferTaskChild> children = transfersService.getAllChildrenTasks(t1);
        Assert.assertEquals(children.size(), 2);

        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();


    }

    @Test(enabled = false)
    public void testDoesTransfer() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(sourceClient);
        //Add some files to transfer
        int FILESIZE = 10 * 1000 * 1024;
        InputStream in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert("a/1.txt", in);
        in = Utils.makeFakeFile(FILESIZE);
        fileOpsService.insert("a/2.txt", in);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            sourceSystem.getName(),
            "/a/",
            destSystem.getName(),
            "/b/"
        );
        // Create a couple of children for the task
        List<TransferTaskChild> kids = new ArrayList<>();
        for(String path : new String[]{"/a/1.txt", "/a/2.txt"}){
            FileInfo fileInfo = new FileInfo();
            fileInfo.setPath(path);
            fileInfo.setSize(FILESIZE);
            TransferTaskChild child = new TransferTaskChild(t1, fileInfo);
            child = transfersService.createTransferTaskChild(child);
            transfersService.publishChildMessage(child);
            kids.add(child);
        }

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        IFileOpsService fileOpsServiceDestination = new FileOpsService(destClient);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED.name());
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
            })
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED.name());
                Assert.assertEquals(k.getBytesTransferred(), FILESIZE);
            })
            .thenCancel()
            .verify();
        List<FileInfo> listing = fileOpsServiceDestination.ls("/b");
        Assert.assertEquals(listing.size(), 2);
        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();
    }

    @Test(enabled = false)
    public void testDoesTransfersWhenOneErrors() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(sourceClient);
        //Add some files to transfer
        InputStream in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/1.txt", in);
        in = Utils.makeFakeFile(10 * 1024);
        fileOpsService.insert("a/2.txt", in);
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            sourceSystem.getName(),
            "/a/",
            destSystem.getName(),
            "/b/"
        );

        List<TransferTaskChild> kids = new ArrayList<>();
        for(String path : new String[]{"/NOT-THERE/1.txt", "/a/2.txt"}){
            FileInfo fileInfo = new FileInfo();
            fileInfo.setPath(path);
            fileInfo.setSize(10 * 1024);
            TransferTaskChild child = new TransferTaskChild(t1, fileInfo);
            child = transfersService.createTransferTaskChild(child);
            transfersService.publishChildMessage(child);
            kids.add(child);
        }
        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        StepVerifier
            .create(stream)
            .assertNext(k->{
                Assert.assertEquals(k.getStatus(), TransferTaskStatus.COMPLETED.name());
            })
            .thenCancel()
            .verify();

        IFileOpsService fileOpsServiceDestination = new FileOpsService(destClient);
        List<FileInfo> listing = fileOpsServiceDestination.ls("/b");
        //NOT-THERE/1.txt should NOT BE THERE
        Assert.assertEquals(listing.size(), 1);
        transfersService.deleteQueue(childQ).subscribe();
        transfersService.deleteQueue(parentQ).subscribe();

    }

    @Test(enabled = false)
    public void testFullPipeline() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(sourceClient);
        //Add some files to transfer
        fileOpsService.insert("/a/1.txt", Utils.makeFakeFile(10000 * 1024));
        fileOpsService.insert("/a/2.txt", Utils.makeFakeFile(10 * 1024));
        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        TransferTask t1 = transfersService.createTransfer(
            "testuser",
            "dev",
            sourceSystem.getName(),
            "/a/",
            destSystem.getName(),
            "/b/"
        );

//        TransferTask t2 = transfersService.createTransfer(
//            "testUser1",
//            "dev",
//            sourceSystem.getId(),
//            "/a/",
//            destSystem.getId(),
//            "/b/"
//        );

        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTask> parentStream = transfersService.processParentTasks(parentMessageStream);
        parentStream.subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        stream.subscribe(taskChild -> log.info(taskChild.toString()));
        IFileOpsService fileOpsServiceDestination = new FileOpsService(destClient);
        // MUST sleep here for a bit for a bit for things to resolve. Alternatively could
        // use a StepVerifier or put some of this in the subscribe callback
        Thread.sleep(2000);
        List<FileInfo> listing = fileOpsServiceDestination.ls("/b");
        Assert.assertEquals(listing.size(), 2);
        t1 = transfersService.getTransferTask(t1.getId());
        Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED.name());
        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }


    @Test(groups = {"performance"}, enabled = false)
    public void testPerformance() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(destSystem);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
        IFileOpsService fileOpsServiceSource = new FileOpsService(sourceClient);
        // 10 tenants with 100 transfers of 10 files
        Map<String, Tenant> tenantMap = new HashMap<>();
        when(tenantManager.getTenants()).thenReturn(tenantMap);

        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        //Add some files to transfer
        fileOpsServiceSource.delete("/");
        for (var i=0;i<10;i++) {
            //Add some files to the source bucket
            String fname = String.format("/a/%s.txt", i);
            fileOpsServiceSource.insert(fname, Utils.makeFakeFile(10000 * 1024));
        }
        for (var i=0;i<1;i++) {
            var tenant = new Tenant();
            tenant.setTenantId("testTenant"+i);
            tenant.setBaseUrl("https://test.tapis.io");
            tenantMap.put(tenant.getTenantId(), tenant);

            for (var j=0;j<1;j++) {
               transfersService.createTransfer(
                    "testuser",
                    "tenant"+i,
                    sourceSystem.getName(),
                    "/a/",
                    destSystem.getName(),
                    "/b/"
                );
            }
        }

        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTask> parentStream = transfersService.processParentTasks(parentMessageStream);
        parentStream.subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        stream
            .take(10)
            .subscribe(taskChild -> {
                log.warn("CURRENT THREAD COUNT: {}", ManagementFactory.getThreadMXBean().getThreadCount() );
                log.info(taskChild.toString());
            });
        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }

    @Test(groups = {"performance"}, enabled = false)
    public void testS3toSSH() throws Exception {
        when(systemsClient.getSystemWithCredentials(eq("sourceSystem"), any())).thenReturn(sourceSystem);
        when(systemsClient.getSystemWithCredentials(eq("destSystem"), any())).thenReturn(testSystemSSH);
        IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(sourceSystem, "testuser");
        IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(destSystem, "testuser");
        IFileOpsService fileOpsServiceSource = new FileOpsService(sourceClient);
        // 10 tenants with 100 transfers of 10 files
        Map<String, Tenant> tenantMap = new HashMap<>();
        when(tenantManager.getTenants()).thenReturn(tenantMap);

        String childQ = UUID.randomUUID().toString();
        transfersService.setChildQueue(childQ);
        String parentQ = UUID.randomUUID().toString();
        transfersService.setParentQueue(parentQ);

        int NUMFILES = 20;
        int NUMTENANTS = 4;
        int NUMTRANSFERS = 1;

        //Add some files to transfer
        fileOpsServiceSource.delete("/");
        for (var i=0;i<NUMFILES;i++) {
            //Add some files to the source bucket
            String fname = String.format("/a/%s.txt", i);
            fileOpsServiceSource.insert(fname, Utils.makeFakeFile(100000 * 1024));
        }
        for (var i=0;i<NUMTENANTS;i++) {
            var tenant = new Tenant();
            tenant.setTenantId("testTenant"+i);
            tenant.setBaseUrl("https://test.tapis.io");
            tenantMap.put(tenant.getTenantId(), tenant);

            for (var j=0;j<NUMTRANSFERS;j++) {
                transfersService.createTransfer(
                    "testuser",
                    "tenant"+i,
                    sourceSystem.getName(),
                    "/a/",
                    destSystem.getName(),
                    "/b/"
                );
            }
        }



        Flux<AcknowledgableDelivery> parentMessageStream = transfersService.streamParentMessages();
        Flux<TransferTask> parentStream = transfersService.processParentTasks(parentMessageStream);
        parentStream
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe();

        Flux<AcknowledgableDelivery> messageStream = transfersService.streamChildMessages();
        Flux<TransferTaskChild> stream = transfersService.processChildTasks(messageStream);
        stream
            .take(10)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(taskChild -> {
                log.info("CURRENT THREAD COUNT = {}", ManagementFactory.getThreadMXBean().getThreadCount());
                log.info(taskChild.toString());
                Assert.assertEquals(taskChild.getBytesTransferred(), 100000 *1024);
            });


        transfersService.deleteQueue(parentQ).subscribe();
        transfersService.deleteQueue(childQ).subscribe();
    }
}
