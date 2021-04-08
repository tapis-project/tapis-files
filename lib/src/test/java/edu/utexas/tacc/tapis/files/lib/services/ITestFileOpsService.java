package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.ResultSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
import jdk.jshell.execution.Util;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.inject.Singleton;
import javax.ws.rs.NotFoundException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.mockito.ArgumentMatchers.any;

/*
 These tests are designed to run against the ssh-machine image in the service's docker-compose file.
 The user is set to be testuser
*/
@Test(groups = {"integration"})
public class ITestFileOpsService {

    private final String oboTenant = "oboTenant";
    private final String oboUser = "oboUser";
    ResultSystem testSystemSSH;
    ResultSystem testSystemS3;
    ResultSystem testSystemPKI;
    private RemoteDataClientFactory remoteDataClientFactory;
    private IFileOpsService fileOpsService;
    private static final Logger log  = LoggerFactory.getLogger(ITestFileOpsService.class);

    private ITestFileOpsService() throws IOException {
        String privateKey = IOUtils.toString(
                this.getClass().getResourceAsStream("/test-machine"),
                StandardCharsets.UTF_8
        );
        String publicKey = IOUtils.toString(
                this.getClass().getResourceAsStream("/test-machine.pub"),
                StandardCharsets.UTF_8
        );
        //SSH system with username/password
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystemSSH = new ResultSystem();
        testSystemSSH.setAuthnCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setId("testSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        List<TransferMethodEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.SFTP);
        testSystemSSH.setTransferMethods(transferMechs);

        // PKI Keys system
        creds = new Credential();
        creds.setPublicKey(publicKey);
        creds.setPrivateKey(privateKey);
        testSystemPKI = new ResultSystem();
        testSystemPKI.setAuthnCredential(creds);
        testSystemPKI.setHost("localhost");
        testSystemPKI.setPort(2222);
        testSystemPKI.setRootDir("/data/home/testuser/");
        testSystemPKI.setId("testSystem");
        testSystemPKI.setEffectiveUserId("testuser");
        transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.SFTP);
        testSystemPKI.setTransferMethods(transferMechs);

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new ResultSystem();
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test");
        testSystemS3.setId("testSystem");
        testSystemS3.setPort(9000);
        testSystemS3.setAuthnCredential(creds);
        testSystemS3.setRootDir("/");
        transferMechs = new ArrayList<>();
        transferMechs.add(TransferMethodEnum.S3);
        testSystemS3.setTransferMethods(transferMechs);
    }

    @DataProvider(name="testSystems")
    public Object[] testSystemsDataProvider () {
        return new ResultSystem[]{
                testSystemSSH,
                testSystemPKI,
                testSystemS3
        };
    }

    @BeforeSuite
    public void doBeforeSuite() {
        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bind(IFileOpsService.class).to(FileOpsService.class).in(Singleton.class);
            }
        });
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
        fileOpsService = locator.getService(FileOpsService.class);
    }

    @BeforeTest()
    public void setUp() {

    }

    @AfterTest()
    public void tearDown() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemSSH, "testuser");
        fileOpsService.delete(client,"/");
        client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystemS3, "testuser");
        fileOpsService.delete(client, "/");
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDelete(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> {
            fileOpsService.ls(client, "/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDeleteNested(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"/a/b/c/test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete(client,"/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> {
            fileOpsService.ls(client,"/a/b/c/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndGet(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"test.txt", in);
        InputStream out = fileOpsService.getStream(client,"test.txt");
        Assert.assertEquals(out.readAllBytes().length,10* 1024);
        out.close();
    }


    @Test(dataProvider = "testSystems")
    public void testListing(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");

    }

    @Test(dataProvider = "testSystems")
    public void testInsertLargeFile(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.insert(client,"test.txt", in);
        List<FileInfo> listing = fileOpsService.ls(client,"test.txt");
        Assert.assertEquals(listing.size(), 1);
        Assert.assertEquals(listing.get(0).getName(), "test.txt");
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

    @Test(dataProvider = "testSystems")
    public void testGetBytesByRange(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.insert(client,"test.txt", in);
        InputStream result = fileOpsService.getBytes(client,"test.txt", 0, 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

    @Test(dataProvider = "testSystems")
    public void testZipFolder(ResultSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(oboTenant, oboUser, testSystem, "testuser");
        fileOpsService.insert(client,"a/test1.txt", Utils.makeFakeFile( 1000 * 1024));
        fileOpsService.insert(client,"a/test2.txt", Utils.makeFakeFile(1000 * 1024));
        File file = File.createTempFile("test", ".zip");
        OutputStream outputStream = new FileOutputStream(file);
        fileOpsService.getZip(client, outputStream, "a/");

        try (FileInputStream fis = new FileInputStream(file);
             ZipInputStream zis = new ZipInputStream(fis);
        ) {
            ZipEntry ze;
            while ( (ze = zis.getNextEntry()) != null) {
                log.info(ze.toString());
                String fname = ze.getName();
                Assert.assertTrue(fname.startsWith("a/test"));
            }

        }
        file.deleteOnExit();

    }

}
