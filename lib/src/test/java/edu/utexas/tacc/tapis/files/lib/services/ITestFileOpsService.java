package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;


import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/*
 These tests are designed to run against the ssh-machine image in the service's docker-compose file.
 The user is set to be testuser
*/
@Test(groups = {"integration"})
public class ITestFileOpsService {

    TSystem testSystemSSH;
    TSystem testSystemS3;
    TSystem testSystemPKI;
    private RemoteDataClientFactory remoteDataClientFactory;

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
        testSystemSSH = new TSystem();
        testSystemSSH.setAccessCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/data/home/testuser/");
        testSystemSSH.setName("testSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        testSystemSSH.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PASSWORD);
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystemSSH.setTransferMethods(transferMechs);

        // PKI Keys system
        creds = new Credential();
        creds.setPublicKey(publicKey);
        creds.setPrivateKey(privateKey);
        testSystemPKI = new TSystem();
        testSystemPKI.setAccessCredential(creds);
        testSystemPKI.setHost("localhost");
        testSystemPKI.setPort(2222);
        testSystemPKI.setRootDir("/data/home/testuser/");
        testSystemPKI.setName("testSystem");
        testSystemPKI.setEffectiveUserId("testuser");
        testSystemPKI.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PKI_KEYS);
        transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystemPKI.setTransferMethods(transferMechs);

        //S3 system
        creds = new Credential();
        creds.setAccessKey("user");
        creds.setAccessSecret("password");
        testSystemS3 = new TSystem();
        testSystemS3.setHost("http://localhost");
        testSystemS3.setBucketName("test");
        testSystemS3.setName("testSystem");
        testSystemS3.setPort(9000);
        testSystemS3.setAccessCredential(creds);
        testSystemS3.setRootDir("/");
        transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.S3);
        testSystemS3.setTransferMethods(transferMechs);
    }

    @DataProvider(name="testSystems")
    public Object[] testSystemsDataProvider () {
        return new TSystem[]{
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
            }
        });
//        locator.inject(this);
        remoteDataClientFactory = locator.getService(RemoteDataClientFactory.class);
    }

    @BeforeTest()
    public void setUp() {

    }

    @AfterTest()
    public void tearDown() throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystemSSH, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        fileOpsService.delete("/");
        client = remoteDataClientFactory.getRemoteDataClient(testSystemS3, "testuser");
        fileOpsService = new FileOpsService(client);
        fileOpsService.delete("/");
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDelete(TSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete("/test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> {
            FileOpsService fos = new FileOpsService(client);
            fos.ls("/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDeleteNested(TSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/a/b/c/test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete("/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> {
            FileOpsService fos = new FileOpsService(client);
            fos.ls("/a/b/c/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndGet(TSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        InputStream out = fileOpsService.getStream("test.txt");
        Assert.assertEquals(out.readAllBytes().length,10* 1024);
        out.close();
    }


    @Test(dataProvider = "testSystems")
    public void testListing(TSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("test.txt");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertTrue(listing.get(0).getName().equals("test.txt"));

    }

    @Test(dataProvider = "testSystems")
    public void testInsertLargeFile(TSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("test.txt");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertTrue(listing.get(0).getName().equals("test.txt"));
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

    @Test(dataProvider = "testSystems")
    public void testGetBytesByRange(TSystem testSystem) throws Exception {
        IRemoteDataClient client = remoteDataClientFactory.getRemoteDataClient(testSystem, "testuser");
        IFileOpsService fileOpsService = new FileOpsService(client);
        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.insert("test.txt", in);
        InputStream result = fileOpsService.getBytes("test.txt", 0, 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

}
