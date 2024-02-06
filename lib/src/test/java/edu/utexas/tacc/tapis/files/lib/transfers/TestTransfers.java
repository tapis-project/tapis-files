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
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
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

import java.io.IOException;
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

  private static final int FILESIZE = 10 * 1024;
  private static final int BIGFILESIZE = 1000 * FILESIZE;

  // Data clients for system specific tests.
  IRemoteDataClient clientSSHa, clientSSHb, clientIRODSa, clientIRODSb, clientS3a, clientS3b, clientS3c, clientS3BucketC;

  public TestTransfers() throws Exception { super(); }

  @BeforeMethod
  public void setUpQueues()
  {
    Utils.clearSshSessionPoolInstance();
    SshSessionPool.init();
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
  public void initMocks() throws Exception
  {
    Mockito.reset(skClient);
    Mockito.reset(serviceClients);
    Mockito.reset(systemsCache);
    Mockito.reset(systemsCacheNoAuth);
    when(serviceClients.getClient(any(String.class), any(String.class), eq(SKClient.class))).thenReturn(skClient);
    when(skClient.isPermitted(any(), any(), any())).thenReturn(true);
  }
  @BeforeMethod
  public void beforeMethod(Method method) { log.info("method name:" + method.getName()); }

  @AfterMethod
  @BeforeMethod
  public void tearDown() throws Exception
  {
    Mockito.reset(permsService);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    for (TapisSystem system: testSystems)
    {
      IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, system);
      cleanupAll(client, system);
    }
  }

  /*
   * Test transfers between two LINUX systems.
   * Use explicit systems rather than a data provider, so we can verify the behavior in detail.
   * We want to explicitly test each case:
   * LINUX/IRODS to LINUX/IRODS
   *   A recursive listing is made for the directory on the LINUX *sourceUri* system
   *   and the files and directory structure are replicated on the *destinationUri* system.
   */
  @Test
  public void testLinux_Linux() throws Exception
  {
    List<FileInfo> listing;
    // Source systems are always the "a" versions: testSystemSSHa, testSystemS3a
    //   and destination systems are the "b" versions.
    // After each transfer test the destination system is reset.
    // Create clients, init mocking, create files
    initSystemSpecific(false);
    // LINUX to LINUX
    // Test txfr of a directory that contains several files in a sub-dir to a sub-dir on another system
    /* SOURCE FILES:
     *   ssha/test0.txt
     *   ssha/a/test1.txt
     *   ssha/a/test2.txt
     *   ssha/a/b/file0_1.txt
     *   ssha/a/b/dir1/file1_1.txt
     *   ssha/a/b/dir2/file2_1.txt
     *   ssha/a/b/dir2/file2_2.txt
     *   ssha/a/b/dir2/file2_3.txt  */
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to LINUX target dir DOES NOT exist     *******************************");
    System.out.println("********************************************************************************************");
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
    printListing(clientSSHb, testSystemSSHb, "/sshb");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 8); // 8 = 7 txfrd plus top level dir since we are listing /sshb
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");

//    System.out.println("********************************************************************************************");
//    System.out.println("************    LINUX to LINUX target dir DOES exist         *******************************");
//    System.out.println("********************************************************************************************");
//    fileOpsService.mkdir(clientSSHb, "sshb/b");
//    runTxfr(testSystemSSHa, "ssha/a/b", testSystemSSHb, "sshb/b", 7, clientSSHb);
//    printListing(clientSSHb, testSystemSSHb, "/sshb");
//    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb/b", MAX_RECURSION);
//    // Because target dir b already exists posix creates another dir b when using cp -r, resulting in dir /sshb/b/b
//    // TODO/TBD: Current we are not doing that. Should we?
//    //   so this fails, because there is not a b under /sshb/b
//    Assert.assertEquals(listing.size(), 8); // 8 = 7 txfrd plus top level dir since we are listing /sshb
//    // Reset destination system.
//    fileOpsService.delete(clientSSHb, "sshb");
//    fileOpsService.mkdir(clientSSHb, "sshb");

//
//    // LINUX to IRODS
//    // Test txfr of a directory that contains several files in a sub-dir to a sub-dir on another system
//    // Surce system SSHa already set up.
//    System.out.println("********************************************************************************************");
//    System.out.println("************    LINUX to IRODS                               *******************************");
//    System.out.println("********************************************************************************************");
//    runTxfr(testSystemSSHa, "ssha/a/b", testSystemIRODSb, "irodsb/dir_from_ssh_a_slash_b", 7, clientIRODSb);
//    listing = fileOpsService.lsRecursive(clientIRODSb, "/irodsb", MAX_RECURSION);
//    Assert.assertEquals(listing.size(), 8);
//    // Reset destination system.
//    fileOpsService.delete(clientIRODSb, "/irodsb");
//    fileOpsService.mkdir(clientIRODSb, "irodsb");
  }

  /*
   * Test transfers between LINUX, S3.
   * Use explicit systems rather than a data provider, so we can verify the behavior in detail.
   * We want to explicitly test each case:
   * S3 to LINUX
   *   All objects matching the *sourceUri* path as a prefix will be created as files on the *destinationUri* system.
   */
  @Test
  public void testS3_Linux() throws Exception
  {
    List<FileInfo> listing;
    // Source systems are always the "a" versions: testSystemSSHa, testSystemS3a
    //   and destination systems are the "b" versions.
    // After each transfer test the destination system is reset.
    // Create clients, init mocking, create files
    initSystemSpecific(false);

    // S3 to LINUX single file
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
    System.out.println("************    S3 to LINUX single file                      *******************************");
    System.out.println("********************************************************************************************");
    /* SOURCE FILES:
     *   a/b/file1.txt
     *   a/b/file2.txt
     */
    // Now txfr /a/b/file1.txt from S3a to SSHb. Only one new file should be created. It should be named "file_from_s3a.txt"
    runTxfr(testSystemS3a, "a/b/file1.txt", testSystemSSHb, "sshb/s3_txfr/file_from_s3a.txt", 1, clientSSHb);
    printListing(clientSSHb, testSystemSSHb, "/sshb/s3_txfr");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 2);
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "/sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");

    // S3 to LINUX multiple files
    System.out.println("********************************************************************************************");
    System.out.println("************    S3 to LINUX multiple files                   *******************************");
    System.out.println("********************************************************************************************");
    // Now txfr /a/b from S3a to SSHb.
    runTxfr(testSystemS3a, "a/b", testSystemSSHb, "sshb/s3_txfr", 2, clientSSHb);
    printListing(clientSSHb, testSystemSSHb, "/sshb/s3_txfr");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    printListing(clientSSHb, testSystemSSHb, "/sshb");
    // We should have 2 files and 1 directory at destination
    Assert.assertEquals(listing.size(), 3);
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "/sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");
  }

  /*
   * Test transfers between LINUX, S3.
   * Use explicit systems rather than a data provider, so we can verify the behavior in detail.
   * We want to explicitly test each case:
   * LINUX/ to S3
   *   A recursive listing is made for the directory on the *sourceUri* system
   *   and for each entry that is not a directory an object is created on the S3 *destinationUri* system.
   */
  @Test
  public void testLinux_S3() throws Exception
  {
    List<FileInfo> listing;
    // Source systems are always the "a" versions: testSystemSSHa, testSystemS3a
    //   and destination systems are the "b" versions.
    // After each transfer test the destination system is reset.
    // Create clients, init mocking, create files
    initSystemSpecific(false);

    // LINUX to S3
    // Test txfr of a directory that contains several files and an empty directory to a system of type S3
    // In addition to the files and directories created on SSHa previously, create an empty directory
    // Since it is a directory this entry should not be transferred to the S3 system
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to S3                                  *******************************");
    System.out.println("********************************************************************************************");
    fileOpsService.mkdir(clientSSHa, "ssha/a/b/dir3");

    // Now txfr /a SSHa to S3b.
    // After txfr destination path should have 5 entries in destination dir files_from_ssha
    //   /file0_1.txt
    //   /dir1/file1_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    runTxfr(testSystemSSHa, "ssha/a/b", testSystemS3b, "files_from_ssha/", 5, clientS3b);
    printListing(clientS3b, testSystemS3b, "");
    listing = fileOpsService.lsRecursive(clientS3b, "/", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 5);
    // Reset destination system.
    cleanupAll(clientS3b, testSystemS3b);

    // LINUX to S3 with S3 rootDir
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to S3 with rootDir                     *******************************");
    System.out.println("********************************************************************************************");
    runTxfr(testSystemSSHa, "ssha/a/b", testSystemS3c, "files_from_ssha/", 5, clientS3c);
    printListing(clientS3c, testSystemS3c, "");
    listing = fileOpsService.lsRecursive(clientS3c, "/", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 5);
    System.out.printf("************ Listing all keys in bucket: %s%n", testSystemS3BucketC.getBucketName());
    printListing(clientS3BucketC, testSystemS3BucketC, "");

    // Reset destination system.
    cleanupAll(clientS3c, testSystemS3c);
  }

  /*
  TODO: This is the original test method with everything. In the process of breaking the tests out into
        separate test methods. See test methods above. Keeping this one unchanged for now.
   * Test transfers between LINUX, S3 and IRODS.
   * Use explicit systems rather than a data provider, so we can verify the behavior in detail.
   * We want to explicitly test each case:
   * LINUX to LINUX/IRODS
   *   A recursive listing is made for the directory on the LINUX *sourceUri* system
   *   and the files and directory structure are replicated on the *destinationUri* system.
   * S3 to LINUX/IRODS
   *   All objects matching the *sourceUri* path as a prefix will be created as files on the *destinationUri* system.
   * LINUX/IRODS to S3
   *   A recursive listing is made for the directory on the *sourceUri* system
   *   and for each entry that is not a directory an object is created on the S3 *destinationUri* system.
   * S3 to S3
   *   All objects matching the *sourceUri* path as a prefix will be re-created as objects on the *destinationUri* system.
   */
  @Test
  public void testLinux_S3_Irods() throws Exception
  {
    List<FileInfo> listing;

    // Source systems are always the "a" versions: testSystemSSHa, testSystemS3a
    //   and destination systems are the "b" versions.
    // After each transfer test the destination system is reset.

    // Create clients, init mocking, create files
    initSystemSpecific(true);

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
    printListing(clientSSHb, testSystemSSHb, "/sshb");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 8);
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");

    // LINUX to IRODS
    // Test txfr of a directory that contains several files in a sub-dir to a sub-dir on another system
    // Surce system SSHa already set up.
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to IRODS                               *******************************");
    System.out.println("********************************************************************************************");
    runTxfr(testSystemSSHa, "ssha/a/b", testSystemIRODSb, "irodsb/dir_from_ssh_a_slash_b", 7, clientIRODSb);
    listing = fileOpsService.lsRecursive(clientIRODSb, "/irodsb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 8);
    // Reset destination system.
    fileOpsService.delete(clientIRODSb, "/irodsb");
    fileOpsService.mkdir(clientIRODSb, "irodsb");

    // S3 to LINUX single file
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
    System.out.println("************    S3 to LINUX single file                      *******************************");
    System.out.println("********************************************************************************************");
    printListing(clientS3a, testSystemS3a, "");

    // Now txfr /a/b/file1.txt from S3a to SSHb. Only one new file should be created. It should be named "file_from_s3a.txt"
    runTxfr(testSystemS3a, "a/b/file1.txt", testSystemSSHb, "sshb/s3_txfr/file_from_s3a.txt", 1, clientSSHb);
    printListing(clientSSHb, testSystemSSHb, "/sshb/s3_txfr");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 2);
    // Reset destination system.
    fileOpsService.delete(clientSSHb, "/sshb");
    fileOpsService.mkdir(clientSSHb, "sshb");

    // S3 to IRODS single file
    System.out.println("********************************************************************************************");
    System.out.println("************    S3 to IRODS                                  *******************************");
    System.out.println("********************************************************************************************");
    // Now txfr /a/s3_afile1.txt from S3a to IRODSb. Only one new file should be created. It should be named "file_from_s3a.txt"
    runTxfr(testSystemS3a, "a/b/file1.txt", testSystemIRODSb, "irodsb/s3_txfr/file_from_s3a.txt", 1, clientIRODSb);
    printListing(clientIRODSb, testSystemIRODSb, "/irodsb/s3_txfr");
    listing = fileOpsService.lsRecursive(clientIRODSb, "/irodsb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 2);
    // Reset destination system.
    fileOpsService.delete(clientIRODSb, "/irodsb");
    fileOpsService.mkdir(clientIRODSb, "irodsb");

    // S3 to LINUX multiple files
    System.out.println("********************************************************************************************");
    System.out.println("************    S3 to LINUX multiple files                   *******************************");
    System.out.println("********************************************************************************************");
    printListing(clientS3a, testSystemS3a, "");

    // TODO/TBD Create the target directory that will contain the files.
    //    Without this each source file is transferred to the file sshb/s3_txfr. The first txfr creates the file
    //    and the second txfr overwrites the same file.
// TODO    fileOpsService.mkdir(clientSSHb, "sshb/s3_txfr");

    // Now txfr /a/b from S3a to SSHb.
    runTxfr(testSystemS3a, "a/b", testSystemSSHb, "sshb/s3_txfr", 2, clientSSHb);
    printListing(clientSSHb, testSystemSSHb, "/sshb/s3_txfr");
    listing = fileOpsService.lsRecursive(clientSSHb, "/sshb", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    printListing(clientSSHb, testSystemSSHb, "/sshb");
    // We should have 2 files and 1 directory at destination
    Assert.assertEquals(listing.size(), 3);
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
    printListing(clientS3b, testSystemS3b, "");
    listing = fileOpsService.lsRecursive(clientS3b, "/", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 5);
    // Reset destination system.
    cleanupAll(clientS3b, testSystemS3b);

    // IRODS to S3
    System.out.println("********************************************************************************************");
    System.out.println("************    IRODS to S3                                  *******************************");
    System.out.println("********************************************************************************************");
    fileOpsService.mkdir(clientIRODSa, "irodsa/a/b/dir3");
    printListing(clientIRODSa, testSystemIRODSa, "/");

    // Now txfr /a IRODSa to S3b.
    // After txfr destination path should have 5 entries in destination dir files_from_irodsa
    //   /file0_1.txt
    //   /dir1/file1_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    //   /dir2/file2_1.txt
    runTxfr(testSystemIRODSa, "irodsa/a/b", testSystemS3b, "files_from_irodsa/", 5, clientS3b);
    printListing(clientS3b, testSystemS3b, "");
    listing = fileOpsService.lsRecursive(clientS3b, "/", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 5);
    // Reset destination system.
    cleanupAll(clientS3b, testSystemS3b);

    // S3 to S3
    System.out.println("********************************************************************************************");
    System.out.println("************    S3 to S3                                     *******************************");
    System.out.println("********************************************************************************************");

    //Add an object to system S3a.
    fileOpsService.upload(clientS3a, "a/b/file3.txt", Utils.makeFakeFile(FILESIZE));
    printListing(clientS3a, testSystemS3a, "");

    // Now txfr /a/b/file3.txt from S3a to S3b.
    runTxfr(testSystemS3a, "a/b/file3.txt", testSystemS3b, "a/b/c/file_from_s3a_file3.txt", 1, clientS3b);
    printListing(clientS3b, testSystemS3b, "");
    listing = fileOpsService.lsRecursive(clientS3b, "/", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 1);

    // LINUX to S3 with S3 rootDir
    System.out.println("********************************************************************************************");
    System.out.println("************    LINUX to S3 with rootDir                     *******************************");
    System.out.println("********************************************************************************************");
    printListing(clientSSHa, testSystemSSHa, "/");

    runTxfr(testSystemSSHa, "ssha/a/b", testSystemS3c, "files_from_ssha/", 5, clientS3c);
    printListing(clientS3c, testSystemS3c, "");
    listing = fileOpsService.lsRecursive(clientS3c, "/", false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 5);

    // Reset destination system.
    cleanupAll(clientS3c, testSystemS3c);
  }

  @Test(dataProvider = "testSystemsDataProvider", groups = {"broken"})
  public void testNotPermitted(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);

    TransferTaskRequestElement element = new TransferTaskRequestElement();
    element.setSourceURI("tapis://sourceSystem/");
    element.setDestinationURI("tapis://destSystem/");
    List<TransferTaskRequestElement> elements = new ArrayList<>();
    elements.add(element);
    // Allow txfr task to be created
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    TransferTask t1 = transfersService.createTransfer(rTestUser, "tag", elements);
    // When not allowed task should be FAILED after the pipeline runs
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(false);
    Flux<TransferTaskParent> tasks = parentTaskTransferService.runPipeline();
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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    fileOpsService.upload(sourceClient,"1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient,"2.txt", Utils.makeFakeFile(FILESIZE));

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
    Assert.assertEquals(task.getTotalBytes(), 2 * FILESIZE);

  }

  @Test(dataProvider = "testSystemsDataProvider")
  public void testDoesListingAndCreatesChildTasks(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    fileOpsService.upload(sourceClient,"1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient,"2.txt", Utils.makeFakeFile(FILESIZE));

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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);

    fileOpsService.upload(sourceClient,"a/1.txt", Utils.makeFakeFile(FILESIZE));

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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    fileOpsService.upload(sourceClient,"a/1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient,"a/2.txt", Utils.makeFakeFile(FILESIZE));

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
    printListing(destClient, destSystem, "");
    Mockito.reset(systemsCache);
    Mockito.reset(systemsCacheNoAuth);
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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);

    fileOpsService.upload(sourceClient,"a/1.txt", Utils.makeFakeFile(FILESIZE));
    fileOpsService.upload(sourceClient,"a/2.txt", Utils.makeFakeFile(FILESIZE));
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

    List<FileInfo> listing = fileOpsService.ls(destClient, "/dest/b/c/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertTrue(listing.size() > 0);
  }

  /*
   * This test is important, basically testing a simple but complete transfer. We check the entries in the database
   * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
   * be fixed.
   */
  @Test(dataProvider = "testSystemsDataProvider", groups = {"broken"})
  public void testDoesTransfer(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);

    //Add some files to transfer
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
    List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    // Create data clients for each system.
    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);
    cleanupAll(sourceClient, sourceSystem);

    //Add some files to transfer
    fileOpsService.upload(sourceClient, "program.exe", Utils.makeFakeFile(BIGFILESIZE));
    boolean recurseFalse = false;
    boolean sharedAppCtxFalse = false;
// TODO sharedCtxGrantor
    fileUtilsService.linuxOp(sourceClient, "/program.exe", FileUtilsService.NativeLinuxOperation.CHMOD, "755",
                             recurseFalse);//, sharedAppCtxFalse);

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

    List<FileInfo> listing = fileOpsService.ls(destClient, "program.exe", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 1);
    Assert.assertTrue(listing.get(0).getNativePermissions().contains("x"));
  }

  /*
   * This test is important, basically testing a simple but complete transfer. We check the entries in the database
   * as well as the files at the destination to make sure it actually completed. If this test fails, something needs to
   * be fixed.
   * NOTE: Test all system pairs except those involving S3 since S3 does not support directories
   */
  @Test(dataProvider = "testSystemsDataProviderNoS3", groups = {"broken"})
  public void testNestedDirectories(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);

    //Add some files to transfer
    fileOpsService.upload(sourceClient, "a/cat/dog/1.txt", Utils.makeFakeFile(BIGFILESIZE));
    fileOpsService.upload(sourceClient, "a/cat/dog/2.txt", Utils.makeFakeFile(BIGFILESIZE));

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
    Assert.assertEquals(parent.getBytesTransferred(), 2 * BIGFILESIZE);

    List<FileInfo> listing = fileOpsService.ls(destClient, "/b/cat/dog/", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 2);
    printListing(destClient, destSystem, "");
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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);

    //Add some files to transfer
    fileOpsService.upload(sourceClient, "a/1.txt", Utils.makeFakeFile(BIGFILESIZE));


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
  @Test(dataProvider = "testSystemsListDataProvider", groups = {"broken"})
  public void testHttpSource(TapisSystem testSystem) throws Exception
  {
    System.out.println("********************************************************************************************");
    System.out.printf("************* HTTP to %s\n", testSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("testSystem"), any(), any(), any())).thenReturn(testSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(testSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystem);

    // Clean up from any previous runs
    cleanupAll(destClient, testSystem);

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

    List<FileInfo> listing = fileOpsService.ls(destClient,"/b", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 1);
    listing = fileOpsService.ls(destClient,"/b/labrador.jpg", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 1);
    printListing(destClient, testSystem, "");
  }


  @Test(dataProvider = "testSystemsDataProvider", groups = {"broken"})
  public void testDoesTransferAtRoot(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);
    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);


    //Add some files to transfer
    fileOpsService.upload(sourceClient, "file1.txt", Utils.makeFakeFile(BIGFILESIZE));

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
              Assert.assertEquals(k.getBytesTransferred(), BIGFILESIZE);
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

    List<FileInfo> listing = fileOpsService.ls(destClient, "/transferred", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 1);
  }

  @Test(dataProvider = "testSystemsDataProvider", groups = {"broken"})
  public void testTransferSingleFile(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    // Double check that the files really are in the destination
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);


    //Add some files to transfer
    InputStream in = Utils.makeFakeFile(BIGFILESIZE);
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
              Assert.assertEquals(k.getBytesTransferred(), BIGFILESIZE);
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

    List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);

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
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);

    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);

    //Add some files to transfer
    InputStream in = Utils.makeFakeFile(FILESIZE);
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
      fileInfo.setSize(FILESIZE);
      fileInfo.setType(FileInfo.FileType.FILE);
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

//  @Test(dataProvider = "testSystemsDataProvider1")
  @Test(dataProvider = "testSystemsDataProviderNoS3", groups = {"broken"})
  public void test10Files(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");

    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
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

    List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 10);
    t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
    Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
    printListing(destClient, destSystem, "");
  }

  @Test(dataProvider = "testSystemsListDataProvider", groups = {"broken"})
  public void testSameSystemForSourceAndDest(TapisSystem testSystem) throws Exception
  {
    TapisSystem sourceSystem = testSystem;
    TapisSystem destSystem = testSystem;
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);

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

    printListing(destClient, destSystem, "");
    List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 2);
    t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
    Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
  }


  // This test follows same pattern as testDoesTransfer
  @Test(dataProvider = "testSystemsDataProvider", groups = {"broken"})
  public void testFullPipeline(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);
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

    List<FileInfo> listing = fileOpsService.ls(destClient, "/b", MAX_LISTING_SIZE, 0, IRemoteDataClient.NO_REGEX);
    Assert.assertEquals(listing.size(), 8);
    t1 = transfersService.getTransferTaskByUUID(t1.getUuid());
    Assert.assertEquals(t1.getStatus(), TransferTaskStatus.COMPLETED);
  }

  // Difficult to test cancel, need for txfrs to have started but not finished before attempting to cancel
  // Tricky timing
  // Txfrs involving S3 frequently time out, so skip them for now.
  @Test(dataProvider = "testSystemsDataProviderNoS3", groups = {"broken"})
  public void testCancelMultipleTransfers(Pair<TapisSystem, TapisSystem> systemsPair) throws Exception
  {
    TapisSystem sourceSystem = systemsPair.getLeft();
    TapisSystem destSystem = systemsPair.getRight();
    System.out.println("********************************************************************************************");
    System.out.printf("************* %s to %s\n", sourceSystem.getId(), destSystem.getId());
    System.out.println("********************************************************************************************");
    when(systemsCache.getSystem(any(), eq("sourceSystem"), any(), any(), any())).thenReturn(sourceSystem);
    when(systemsCache.getSystem(any(), eq("destSystem"), any(), any(), any())).thenReturn(destSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("sourceSystem"), any())).thenReturn(sourceSystem);
    when(systemsCacheNoAuth.getSystem(any(), eq("destSystem"), any())).thenReturn(destSystem);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    IRemoteDataClient sourceClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, sourceSystem);
    IRemoteDataClient destClient = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, destSystem);

    //wipe out the dest folder just in case
    cleanupAll(destClient, destSystem);

    //Add some files to transfer
    fileOpsService.upload(sourceClient, "a/file1.txt", Utils.makeFakeFile(BIGFILESIZE));
    fileOpsService.upload(sourceClient, "a/file2.txt", Utils.makeFakeFile(BIGFILESIZE));
    fileOpsService.upload(sourceClient, "a/file3.txt", Utils.makeFakeFile(BIGFILESIZE));
    fileOpsService.upload(sourceClient, "a/file4.txt", Utils.makeFakeFile(BIGFILESIZE));
    fileOpsService.upload(sourceClient, "a/file5.txt", Utils.makeFakeFile(BIGFILESIZE));

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
   * If checkPaths=true then confirm that we can retrieve each file from the path contained in FileInfo
   */
  private void printListing(IRemoteDataClient client, TapisSystem system, String path) throws ServiceException, IOException
  {
    printListing(client, system, path, false);
  }
  private void printListing(IRemoteDataClient client, TapisSystem system, String path, boolean checkPaths)
          throws ServiceException, IOException
  {
    System.out.println("============================================================================================");
    System.out.printf("Listing for system: %s rootDir: %s path: %s%n", system.getId(), system.getRootDir(), path);
    System.out.println("--------------------------------------------------------------------------------------------");
    List<FileInfo> listing = fileOpsService.lsRecursive(client, path, false, MAX_RECURSION, IRemoteDataClient.NO_REGEX);
    for (FileInfo fi1 : listing)
    {
      System.out.println(fi1);
      if (checkPaths)
      {
        // Confirm that we can retrieve the file using the path from FileInfo
        String fiPath = fi1.getPath();
        System.out.printf("***** Checking path for file: %s%n", fiPath);
        Assert.assertFalse(StringUtils.isBlank(fiPath));
        FileInfo fi2 = client.getFileInfo(fiPath, true);
        Assert.assertNotNull(fi2);
        System.out.println(fi2);
        // Confirm attributes
        Assert.assertEquals(fi2.getName(), fi1.getName());
        Assert.assertEquals(fi2.getPath(), fi1.getPath());
        Assert.assertEquals(fi2.getSize(), fi1.getSize());
        Assert.assertEquals(fi2.getType(), fi1.getType());
        Assert.assertEquals(fi2.getUrl(), fi1.getUrl());
      }
    }
    System.out.println("============================================================================================");
  }

  /*
   * Execute and validate a transfer
   */
  private void runTxfr(TapisSystem srcSystem, String srcPath, TapisSystem dstSystem, String dstPath, int numExpected,
                       IRemoteDataClient dstClient) throws ServiceException, IOException
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
    printListing(dstClient, dstSystem, dstPath, true);
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

  // Utility method to remove all files/objects given the client. Need to handle S3
  void cleanupAll(IRemoteDataClient client, TapisSystem sys) throws ServiceException
  {
    if (SystemTypeEnum.S3.equals(sys.getSystemType()))
    {
      fileOpsService.delete(client, "");
    }
    else
    {
      fileOpsService.delete(client, "/");
    }
  }

  // Init for system specific tests
  // Create clients, init mocking, create files
  void initSystemSpecific(boolean doIrods) throws IOException, ServiceException
  {
    // Source systems are always the "a" versions: testSystemSSHa, testSystemS3a
    //   and destination systems are the "b" versions.

    // Create data clients for each system.
    clientSSHa = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSHa);
    clientSSHb = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemSSHb);
    clientS3a = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3a);
    clientS3b = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3b);
    clientS3c = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3c);
    clientS3BucketC = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemS3BucketC);
    if (doIrods) clientIRODSa = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemIRODSa);
    if (doIrods) clientIRODSb = remoteDataClientFactory.getRemoteDataClient(devTenant, testUser, testSystemIRODSb);

    // Init mocking to return values appropriate to the test
    when(systemsCache.getSystem(any(), eq(testSystemSSHa.getId()), any(), any(), any())).thenReturn(testSystemSSHa);
    when(systemsCache.getSystem(any(), eq(testSystemSSHb.getId()), any(), any(), any())).thenReturn(testSystemSSHb);
    when(systemsCache.getSystem(any(), eq(testSystemS3a.getId()), any(), any(), any())).thenReturn(testSystemS3a);
    when(systemsCache.getSystem(any(), eq(testSystemS3b.getId()), any(), any(), any())).thenReturn(testSystemS3b);
    when(systemsCache.getSystem(any(), eq(testSystemS3c.getId()), any(), any(), any())).thenReturn(testSystemS3c);
    if (doIrods) when(systemsCache.getSystem(any(), eq(testSystemIRODSa.getId()), any(), any(), any())).thenReturn(testSystemIRODSa);
    if (doIrods) when(systemsCache.getSystem(any(), eq(testSystemIRODSb.getId()), any(), any(), any())).thenReturn(testSystemIRODSb);
    when(systemsCacheNoAuth.getSystem(any(), eq(testSystemSSHa.getId()), any())).thenReturn(testSystemSSHa);
    when(systemsCacheNoAuth.getSystem(any(), eq(testSystemSSHb.getId()), any())).thenReturn(testSystemSSHb);
    when(systemsCacheNoAuth.getSystem(any(), eq(testSystemS3a.getId()), any())).thenReturn(testSystemS3a);
    when(systemsCacheNoAuth.getSystem(any(), eq(testSystemS3b.getId()), any())).thenReturn(testSystemS3b);
    when(systemsCacheNoAuth.getSystem(any(), eq(testSystemS3c.getId()), any())).thenReturn(testSystemS3c);
    if (doIrods) when(systemsCacheNoAuth.getSystem(any(), eq(testSystemIRODSa.getId()), any())).thenReturn(testSystemIRODSa);
    if (doIrods) when(systemsCacheNoAuth.getSystem(any(), eq(testSystemIRODSb.getId()), any())).thenReturn(testSystemIRODSb);
    when(permsService.isPermitted(any(), any(), any(), any(), any())).thenReturn(true);

    // Cleanup and create top level directories
    cleanupAll(clientSSHa, testSystemSSHa);
    cleanupAll(clientSSHb, testSystemSSHb);
    cleanupAll(clientS3a, testSystemS3a);
    cleanupAll(clientS3b, testSystemS3b);
    cleanupAll(clientS3c, testSystemS3c);
    fileOpsService.mkdir(clientSSHa, "ssha");
    fileOpsService.mkdir(clientSSHb, "sshb");
    if (doIrods)
    {
      cleanupAll(clientIRODSa, testSystemIRODSa);
      cleanupAll(clientIRODSb, testSystemIRODSb);
      fileOpsService.mkdir(clientIRODSa, "irodsa");
      fileOpsService.mkdir(clientIRODSb, "irodsb");
    }

    // Create a set of file paths that can represent a posix directory structure or a list of S3 keys
    // NOTE: TBD: Use this for each test. Might be able to automate some of the verification or setup.
    List<String> filePaths = new ArrayList<>(List.of("emptyDir/",
            "file0",
            "file1",
            "a/emptyDirA/",
            "a/file1a",
            "a/file2a",
            "a/b/emtpyDirB/",
            "a/b/file1b",
            "a/b/file2b",
            "a/b/c/file1c",
            "a/b/c/file2c"));

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
    if (doIrods)
    {
      fileOpsService.upload(clientIRODSa, "irodsa/test0.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/test1.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/test2.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/b/file0_1.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/b/dir1/file1_1.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/b/dir2/file2_1.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/b/dir2/file2_2.txt", Utils.makeFakeFile(FILESIZE));
      fileOpsService.upload(clientIRODSa, "irodsa/a/b/dir2/file2_3.txt", Utils.makeFakeFile(FILESIZE));
    }
  }
}
