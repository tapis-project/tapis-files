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
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
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
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.MAX_RECURSION;
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

  private String childQ;
  private String parentQ;
  private final ResourceRequestUser rTestUser =
          new ResourceRequestUser(new AuthenticatedUser(testUser, devTenant, TapisThreadContext.AccountType.user.name(),
                                                        null, testUser, devTenant, null, null, null));

  public TestTransfers() throws Exception { super(); }

  @BeforeMethod
  public void setUpQueues()
  {
    childQ = UUID.randomUUID().toString();
    transfersService.setChildQueue(childQ).block(Duration.ofSeconds(1));
    parentQ = UUID.randomUUID().toString();
    transfersService.setParentQueue(parentQ).block(Duration.ofSeconds(1));
  }

  @AfterMethod
  public void deleteQueues()
  {
    log.info("Deleting Queue: {}", childQ);
    transfersService.deleteQueue(childQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe((m)-> log.info("Deleted queue {}", childQ),
                       (err)->log.error(err.getMessage(), err));
    log.info("Deleting Queue: {}", parentQ);
    transfersService.deleteQueue(parentQ)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe((m)-> log.info("Deleted queue {}", parentQ),
                       (err)-> log.error(err.getMessage(), err));
  }

  @BeforeMethod
  public void beforeMethod(Method method) {
    log.info("method name:" + method.getName());
  }

  @AfterMethod
  public void initialize()
  {
    Mockito.reset(systemsCache);
    Mockito.reset(serviceClients);
    Mockito.reset(permsService);
  }

  @AfterMethod
  @BeforeMethod
  public void tearDown() throws Exception
  {
    Mockito.reset(permsService);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    for (TapisSystem system: testSystems)
    {
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, system, testUser);
      fileOpsService.delete(client,"/");
    }
  }

  /*
   * Test transfers between Linux and S3.
   * Use explicit systems rather than a data provider, so we can verify the behavior in detail.
   * We want to explicitly test each case:
   * LINUX to LINUX
   *   Transfer of a directory is supported. A recursive listing is made for the directory on the LINUX *sourceUri* system
   *   and the files and directory structure are replicated on the LINUX *destinationUri* system.
   * S3 to LINUX
   *   Transfer of a directory is not supported. The content of the object from the S3 *sourceUri* system is used to
   *   create a single file on the LINUX *destinationUri* system.
   * LINUX to S3
   *   Transfer of a directory is supported. A recursive listing is made for the directory on the LINUX *sourceUri* system
   *   and for each entry that is not a directory an object is created on the S3 *destinationUri* system.
   * S3 to S3
   *   Transfer of a directory is not supported. The content of the object from the S3 *sourceUri* system is used to
   *   create a single object on the S3 *destinationUri* system.
   * HTTP/S to LINUX or S3
   *  Transfer of a directory is not supported. The content of the object from *sourceUri* URL is used to create a
   *  single file or object on the *destinationUri* system.
   */
  @Test
  public void testLinux_S3() throws Exception
  {
    int FILESIZE = 10 * 1024;
    List<FileInfo> listing;

    // Source systems are always the "a" versions: testSystemSSHa, testSystemS3a
    //   and destination systems are the "b" versions.
    // After each transfer test the destination system is reset.

    // Init mocking to return values appropriate to the test
    when(systemsCache.getSystem(any(), eq(testSystemSSHa.getId()), any())).thenReturn(testSystemSSHa);
    when(systemsCache.getSystem(any(), eq(testSystemSSHb.getId()), any())).thenReturn(testSystemSSHb);
    when(systemsCache.getSystem(any(), eq(testSystemS3a.getId()), any())).thenReturn(testSystemS3a);
    when(systemsCache.getSystem(any(), eq(testSystemS3b.getId()), any())).thenReturn(testSystemS3b);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    // Create data clients for each system.
    IRemoteDataClient clientSSHa = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSHa, testUser);
    IRemoteDataClient clientSSHb = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSHb, testUser);
    IRemoteDataClient clientS3a = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3a, testUser);
    IRemoteDataClient clientS3b = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3b, testUser);

    // Cleanup and create top level directories
    fileOpsService.delete(clientSSHa, "/");
    fileOpsService.delete(clientSSHb, "/");
    fileOpsService.delete(clientS3a, "/");
    fileOpsService.delete(clientS3b, "/");
    fileOpsService.mkdir(clientSSHa, "ssha");
    fileOpsService.mkdir(clientSSHb, "sshb");

    // Create a set of file paths that can represent a posix directory structure or a list of S3 keys
    // TODO/TBD: Use this for each test. Might be able to automate some of the verification or setup.
    List<String> filePaths = new ArrayList<>(List.of("file0.txt",
                                                     "file1.txt",
                                                     "a/file1a.txt",
                                                     "a/file2a.txt",
                                                     "a/b/file1b.txt",
                                                     "a/b/file2b.txt",
                                                     "a/b/c/file1c.txt",
                                                     "a/b/c/file2c.txt"));

    // Create files and directories on source systems
    fileOpsService.upload(clientSSHa, "ssha/test0.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/test1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/test2.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/b/file0_1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/b/dir1/file1_1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/b/dir2/file2_1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/b/dir2/file2_2.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientSSHa, "ssha/a/b/dir2/file2_3.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientS3a, "a/b/file1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(clientS3a, "a/b/file2.txt", Utils.makeFakeFile(FILESIZE));

    // LINUX to LINUX
    // Test txfr of a directory that contains several files in a sub-dir to a sub-dir on another system
    // Add some objects to system SSHa.
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to LINUX                               *******************************");
    System.out.println("********************************************************************************************");
    printListing(clientSSHa, testSystemSSHa, "/ssha");

    // Now txfr SSHa:ssha/a/b to SSHb:sshb/ssh_b_dir3_from_ssh_a_slash_b
    // After txfr destination path should have 7 entries in destination dir /sshb/ssh_b_dir3_from_ssh_a_slash_b:
    //   /file0_1.txt
    //   /dir1
    //   /dir1/file1_1.txt
    //   /dir2
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    runTxfr(testSystemSSHa, "ssha/a/b", testSystemSSHb, "sshb/dir_from_ssh_a_slash_b", 7, clientSSHb);
    printListing(clientSSHb, testSystemSSHa, "/sshb");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", MAX_RECURSION);
    Assert.assertEquals(listing.size(), 8);
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "/sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");

    // S3 to LINUX
    // ===============
    // NOTE:
    //   minio does not support creating an object if the prefix already exists as an object.
    //     For example, trying this:
    //       fileOpsService.upload(clientS3a, "/a/s3_a", Utils.makeFakeFile(0));
    //       fileOpsService.upload(clientS3a, "/a/s3_a/file1.txt", Utils.makeFakeFile(FILESIZE));
    //     Results in:
    //       software.amazon.awssdk.services.s3.model.S3Exception: Object-prefix is already an object, please choose a different object-prefix name. (Service: S3, Status Code: 400, Request ID: 171A5C4FFC0E9613)
    //   minio also does not allow this:
    //       fileOpsService.upload(clientS3a, "/a/s3_a/file1.txt", Utils.makeFakeFile(FILESIZE));
    //       fileOpsService.upload(clientS3a, "/a/s3_a", Utils.makeFakeFile(0));
    //     Results in:
    //       software.amazon.awssdk.services.s3.model.S3Exception: Object name already exists as a directory. (Service: S3, Status Code: 409, Request ID: 171A5C36A6694AC6)
    System.out.println("********************************************************************************************");
    System.out.println("************    S3 to LINUX                                  *******************************");
    System.out.println("********************************************************************************************");

    printListing(clientS3a, testSystemS3a, "/");

    // Now txfr /a/s3_afile1.txt from S3a to SSHb. Only one new file should be created. It should be named "file_from_s3a.txt"
    runTxfr(testSystemS3a, "a/b/file1.txt", testSystemSSHb, "sshb/s3b_txfr/file_from_s3a.txt", 1, clientSSHb);
    printListing(clientSSHb, testSystemSSHb, "/sshb/s3b_txfr");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", MAX_RECURSION);
    Assert.assertEquals(listing.size(), 2);
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "/sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");

    // LINUX to S3
    // Test txfr of a directory that contains several files and an empty directory to a system of type S3
    // In addition to the files and directories created on SSHa previously, create an empty directory
    // Since it is a directory this entry should not be transferred to the S3 system
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to S3                                  *******************************");
    System.out.println("********************************************************************************************");
    fileOpsService.mkdir(clientSSHa, "ssha/a/b/dir3");
    printListing(clientSSHa, testSystemSSHa, "/");

    // Now txfr /a SSHa to S3b.
    // After txfr destination path should have 5 entries in destination dir files_from_ssha
    //   /file0_1.txt
    //   /dir1/file1_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    runTxfr(testSystemSSHa, "ssha/a/b", testSystemS3b, "files_from_ssha/", 5, clientS3b);
    printListing(clientS3b, testSystemS3b, "/");
    listing = fileOpsService.lsRecursive(clientS3b, "/", MAX_RECURSION);
    Assert.assertEquals(listing.size(), 5);
    // Reset destination system.
    fileOpsService.delete(clientS3b, "/");

    // S3 to S3
    System.out.println("********************************************************************************************");
    System.out.println("************    S3 to S3                                     *******************************");
    System.out.println("********************************************************************************************");

    //Add an object to system S3a.
    fileOpsService.upload(clientS3a, "a/b/file3.txt", Utils.makeFakeFile(FILESIZE));
    printListing(clientS3a, testSystemS3a, "/");

    // Now txfr /a/b/file3.txt from S3a to S3b.
    runTxfr(testSystemS3a, "a/b/file3.txt", testSystemS3b, "a/b/c/file_from_s3a_file3.txt", 1, clientS3b);
    printListing(clientS3b, testSystemS3b, "/");
    listing = fileOpsService.lsRecursive(clientS3b, "/", MAX_RECURSION);
    Assert.assertEquals(listing.size(), 1);
  }

  @Test(dataProvider = "testSystemsDataProvider")
  public void testNotPermitted(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
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
  public void testTagSaveAndReturned(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
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

  // Note we skip S3 when it is the source because only 1 path is transferred in that case
  @Test(dataProvider = "testSystemsDataProvider")
  public void testUpdatesTransferSize(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();

    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    // Note we skip S3 when it is the source because only 1 path is transferred in that case
    // This test does not make sense for S3 as source. There will always be 1 element per parent task so the parent
    //    task transfer size will never need updating.
    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      System.out.println(("Source is of type S3. Test does not apply."));
      return;
    }
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    fileOpsService.upload(sourceClient,"1.txt", Utils.makeFakeFile(10 * 1024));
    fileOpsService.upload(sourceClient,"2.txt", Utils.makeFakeFile(10 * 1024));

    List<TransferTaskRequestElement> elements = new ArrayList<>();
    TransferTaskRequestElement element;
    element = new TransferTaskRequestElement();
    element.setSourceURI("tapis://sourceSystem/");
    element.setDestinationURI("tapis://destSystem/");
    elements.add(element);

    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    // Task should be STAGED after the pipeline runs
    StepVerifier.create(tasks)
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
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    fileOpsService.upload(sourceClient,"1.txt", Utils.makeFakeFile(10 * 1024));
    fileOpsService.upload(sourceClient,"2.txt", Utils.makeFakeFile(10 * 1024));

    List<TransferTaskRequestElement> elements = new ArrayList<>();
    TransferTaskRequestElement element;
    // When source is of type S3 only one path is transferred, so create 2 elements for that case
    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/1.txt");
      element.setDestinationURI("tapis://destSystem/1.txt");
      elements.add(element);
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/2.txt");
      element.setDestinationURI("tapis://destSystem/2.txt");
      elements.add(element);
    }
    else
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/");
      element.setDestinationURI("tapis://destSystem/");
      elements.add(element);
    }

    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
    // These next 2 tasks must be created for test to pass. Why?
    TransferTask t2 = transfersService.createTransfer(rTestUser, "tag", elements);
    TransferTask t3 = transfersService.createTransfer(rTestUser, "tag", elements);
    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    StepVerifier.create(tasks)
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
  public void testFailsTransferWhenParentFails(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);

    fileOpsService.upload(sourceClient,"a/1.txt", Utils.makeFakeFile(10 * 1024));

    // Set up a txfr that will fail. Source path does not exist
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    TransferTaskRequestElement element = new TransferTaskRequestElement();
    element.setSourceURI("tapis://sourceSystem/a/2.txt");
    element.setDestinationURI("tapis://destSystem/b/2.txt");
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

  @Test(dataProvider = "testSystemsDataProviderNoS3")
  public void testMultipleChildren(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
    printListing(destClient, destSystem, "/");
    Mockito.reset(systemsCache);
  }

  // Test all system pairs except those involving S3 since S3 does not support directories
  @Test(dataProvider = "testSystemsDataProviderNoS3")
  public void testEmptyDirectories(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);

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
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    fileOpsService.delete(destClient, "/");

    //Add some files to transfer
    int FILESIZE = 10 * 1024;
    fileOpsService.upload(sourceClient, "a/1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient, "a/2.txt", Utils.makeFakeFile(FILESIZE));

    TransferTaskRequestElement element;
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    // When source is of type S3 only one path is transferred, so create 2 elements for that case
    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/1.txt");
      element.setDestinationURI("tapis://destSystem/b/1.txt");
      elements.add(element);
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/2.txt");
      element.setDestinationURI("tapis://destSystem/b/2.txt");
      elements.add(element);
    }
    else
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/");
      element.setDestinationURI("tapis://destSystem/b/");
      elements.add(element);
    }

    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    tasks.subscribe();
    Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
// StepVerify is part of reactor test framework (takes over stream and you can examine/assert it)
    StepVerifier.create(stream)
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

  // This test only applies for LINUX to LINUX
  @Test
  public void testTransferExecutable() throws Exception
  {
    TapisSystem sourceSystem = testSystemSSH;
    TapisSystem destSystem = testSystemPKI;
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    // Init mocking to return values appropriate to the test
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    // Create data clients for each system.
    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    fileOpsService.delete(destClient, "/");
    fileOpsService.delete(sourceClient, "/");

    //Add some files to transfer
    int FILESIZE = 10 * 1000 * 1024;
    fileOpsService.upload(sourceClient, "program.exe", Utils.makeFakeFile(FILESIZE));
    boolean recurseFalse = false;
    boolean sharedAppCtxFalse = false;
    fileUtilsService.linuxOp(sourceClient, "/program.exe", FileUtilsService.NativeLinuxOperation.CHMOD, "755",
                             recurseFalse, sharedAppCtxFalse);

    TransferTaskRequestElement element = new TransferTaskRequestElement();
    element.setSourceURI("tapis://sourceSystem/program.exe");
    element.setDestinationURI("tapis://destSystem/program.exe");
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    elements.add(element);
    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    tasks.subscribe();
    Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
    StepVerifier.create(stream)
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
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
    printListing(destClient, destSystem, "/");
  }

  /**
   * Tests to se if the grouping does not hang after 256 groups
   * creates many threads, enable only as needed
   */
  @Test(dataProvider = "testSystemsDataProvider", enabled = false)
  public void testMaxGroupSize(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
   * Transfer a file from https to various Tapis systems.
   * This test is important, basically testing a simple but complete transfer. We check the entries in the database
   * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
   * be fixed.
   */
  @Test(dataProvider = "testSystemsListDataProvider")
  public void testHttpSource(TapisSystem testSystem) throws Exception
  {
    System.out.println("********************************************************************************************");
    System.out.printf("************* HTTP to %s\n", testSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("testSystem"), any())).thenReturn(testSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem, testUser);

    // Clean up from any previous runs
    fileOpsService.delete(destClient, "/");

    // Create the transfer
    TransferTaskRequestElement element = new TransferTaskRequestElement();
    element.setSourceURI("https://s3.amazonaws.com/cdn-origin-etr.akc.org/wp-content/uploads/2017/11/12231410/Labrador-Retriever-On-White-01.jpg");
    element.setDestinationURI("tapis://testSystem/b/labrador.jpg");
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    elements.add(element);
    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

    // Execute the transfer
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

    // Validate task and parentTask properties
    t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
    Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    Assert.assertNotNull(t1.getStartTime());
    Assert.assertNotNull(t1.getEndTime());
    TransferTaskParent parent = t1.getParentTasks().get(0);
    Assert.assertEquals(parent.getStatus(), TransferTaskStatus.COMPLETED);
    Assert.assertNotNull(parent.getEndTime());
    Assert.assertNotNull(parent.getStartTime());
    Assert.assertTrue(parent.getBytesTransferred() > 0);

    List<FileInfo> listing = fileOpsService.ls(destClient,"/b", MAX_LISTING_SIZE, 0);
    Assert.assertEquals(listing.size(), 1);
    listing = fileOpsService.ls(destClient,"/b/labrador.jpg", MAX_LISTING_SIZE, 0);
    Assert.assertEquals(listing.size(), 1);
    printListing(destClient, testSystem, "/");
  }


  @Test(dataProvider = "testSystemsDataProvider")
  public void testDoesTransferAtRoot(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
  public void testTransferSingleFile(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);

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
      TransferTaskChild child = new TransferTaskChild(parent, fileInfo, sourceSystem);
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

  @Test(dataProvider = "testSystemsDataProviderNoS3")
  public void test10Files(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
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
    printListing(destClient, destSystem, "/");
  }

  @Test(dataProvider = "testSystemsListDataProvider")
  public void testSameSystemForSourceAndDest(TapisSystem testSystem) throws Exception
  {
    TapisSystem sourceSystem = testSystem;
    TapisSystem destSystem = testSystem;
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);

    //Add some files to transfer
    fileOpsService.upload(sourceClient, "a/1.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient, "a/2.txt", Utils.makeFakeFile(10000 * 1024));

    List<TransferTaskRequestElement> elements = new ArrayList<>();
    TransferTaskRequestElement element;
    // When source is of type S3 only one path is transferred, so create 2 elements for that case
    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/1.txt");
      element.setDestinationURI("tapis://destSystem/b/1.txt");
      elements.add(element);
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/2.txt");
      element.setDestinationURI("tapis://destSystem/b/2.txt");
      elements.add(element);
    }
    else
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/");
      element.setDestinationURI("tapis://destSystem/b/");
      elements.add(element);
    }
    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    tasks.subscribe();

    Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
    StepVerifier.create(stream)
            .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
            .assertNext(t-> { Assert.assertEquals(t.getStatus(),TransferTaskStatus.COMPLETED); })
            .thenCancel()
            .verify(Duration.ofSeconds(5));

    printListing(destClient, destSystem, "/");
    List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0);
    Assert.assertEquals(listing.size(), 2);
    t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
    Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
  }


  // This test follows same pattern as testDoesTransfer
  @Test(dataProvider = "testSystemsDataProvider")
  public void testFullPipeline(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);
    //Add some files to transfer
    fileOpsService.upload(sourceClient,"/a/1.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/2.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/3.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/4.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/5.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/6.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/7.txt", Utils.makeFakeFile(10000 * 1024));
    fileOpsService.upload(sourceClient,"/a/8.txt", Utils.makeFakeFile(10000 * 1024));

    List<TransferTaskRequestElement> elements = new ArrayList<>();
    TransferTaskRequestElement element;
    // When source is of type S3 only one path is transferred, so create multiple elements for that case
    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      for (int i = 1; i<=8; i++)
      {
        element = new TransferTaskRequestElement();
        element.setSourceURI(String.format("tapis://sourceSystem/a/%d.txt", i));
        element.setDestinationURI(String.format("tapis://destSystem/b/%d.txt", i));
        elements.add(element);
      }
    }
    else
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/");
      element.setDestinationURI("tapis://destSystem/b/");
      elements.add(element);
    }

    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);

    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    tasks.subscribe();

    Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
    StepVerifier.create(stream)
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
  // Txfrs involving S3 frequently time out, so skip them for now.
  @Test(dataProvider = "testSystemsDataProviderNoS3")
  public void testCancelMultipleTransfers(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem, testUser);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem, testUser);

    //wipe out the dest folder just in case
    fileOpsService.delete(destClient, "/");

    //Add some files to transfer
    int FILESIZE = 100 * 1000 * 1024;
    fileOpsService.upload(sourceClient, "a/file1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient, "a/file2.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient, "a/file3.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient, "a/file4.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient, "a/file5.txt", Utils.makeFakeFile(FILESIZE));

    List<TransferTaskRequestElement> elements = new ArrayList<>();
    TransferTaskRequestElement element;
    // When source is of type S3 only one path is transferred, so create multiple elements for that case
    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
    {
      for (int i = 1; i<=5; i++)
      {
        element = new TransferTaskRequestElement();
        element.setSourceURI(String.format("tapis://sourceSystem/a/file%d.txt", i));
        element.setDestinationURI(String.format("tapis://destSystem/b/file%d.txt", i));
        elements.add(element);
      }
    }
    else
    {
      element = new TransferTaskRequestElement();
      element.setSourceURI("tapis://sourceSystem/a/");
      element.setDestinationURI("tapis://destSystem/b/");
      elements.add(element);
    }

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
            .verify(Duration.ofSeconds(30));
  }

  // Make sure we can pick up control messages, such as the cancel control msg
  @Test
  public void testControlMessages() throws Exception
  {
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
  public void testFlux()
  {
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

  private void printTransferTask(TransferTask task)
  {
    System.out.println("================================================================");
    System.out.println("Txfr task with Id: " + task.getId());
    for (TransferTaskParent pt :  task.getParentTasks())
    {
      System.out.println("Parent task: " + pt.toString());
      if (pt.getChildren() == null) return;
      System.out.println("Parent task children:");
      for (TransferTaskChild child : pt.getChildren())
      {
        System.out.println("Child task: " + child.toString());
      }
    }
    System.out.println("================================================================");
  }

  /*
   * Use FileOps service to list files at a path and print them out.
   */
  private void printListing(IRemoteDataClient client, TapisSystem system, String path) throws ServiceException
  {
    System.out.println("============================================================================================");
    System.out.printf("Listing for system: %s rootDir: %s path: %s%n", system.getId(), system.getRootDir(), path);
    System.out.println("--------------------------------------------------------------------------------------------");
    List<FileInfo> listing = fileOpsService.lsRecursive(client, path, MAX_RECURSION);
    for (FileInfo fi : listing)
    {
      System.out.println(fi.toString());
    }
    System.out.println("============================================================================================");
  }

  /*
   * Execute and validate a transfer
   */
  private void runTxfr(TapisSystem srcSystem, String srcPath, TapisSystem dstSystem, String dstPath, int numExpected,
                       IRemoteDataClient dstClient) throws ServiceException
  {
    TransferTaskRequestElement element = new TransferTaskRequestElement();
    element.setSourceURI(String.format("tapis://%s/%s", srcSystem.getId(), srcPath));
    element.setDestinationURI(String.format("tapis://%s/%s", dstSystem.getId(), dstPath));
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    elements.add(element);
    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
    printTransferTask(t1);
    // Run the txfr
    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
    tasks.subscribe();
    Flux<TransferTaskChild> stream = childTaskTransferService.runPipeline();
    // Instead of using StepVerifier, block until everything is done.
    // blockLast - start listening to stream and wait for it to finish.
    stream.take(Duration.ofSeconds(5)).blockLast();

    // Get the txfr task and check properties
    t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
    printTransferTask(t1);
    // Print listing so manual check can be done even if txfr fails
    printListing(dstClient, dstSystem, dstPath);
    // Confirm that txfr succeeded
    Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    Assert.assertNotNull(t1.getStartTime());
    Assert.assertNotNull(t1.getEndTime());
    TransferTaskParent parent = t1.getParentTasks().get(0);
    Assert.assertEquals(parent.getStatus(), TransferTaskStatus.COMPLETED);
    Assert.assertNotNull(parent.getEndTime());
    Assert.assertNotNull(parent.getStartTime());
    // confirm number of items transferred
    Assert.assertEquals(t1.getTotalTransfers(), numExpected);
    Assert.assertEquals(t1.getCompleteTransfers(), numExpected);
  }
// TODO
//  List<TransferTaskRequestElement> elements = new ArrayList<>();
//  TransferTaskRequestElement element;
//  // When source is of type S3 only one path is transferred, so create 2 elements for that case
//    if (SystemTypeEnum.S3.equals(sourceSystem.getSystemType()))
//  {
//    element = new TransferTaskRequestElement();
//    element.setSourceURI("tapis://sourceSystem/1.txt");
//    element.setDestinationURI("tapis://destSystem/1.txt");
//    elements.add(element);
//    element = new TransferTaskRequestElement();
//    element.setSourceURI("tapis://sourceSystem/2.txt");
//    element.setDestinationURI("tapis://destSystem/2.txt");
//    elements.add(element);
//  }
//    else
//  {
//    element = new TransferTaskRequestElement();
//    element.setSourceURI("tapis://sourceSystem/");
//    element.setDestinationURI("tapis://destSystem/");
//    elements.add(element);
//  }
}
