package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.files.lib.Utils;
import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.clients.SSHDataClient;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.NotFoundException;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/*
 These tests are designed to run against the ssh-machine image in the service's docker-compose file.
 The user is set to be testuser
*/
@Test(groups = {"integration"})
public class ITestFileOpsService {
    FileOpsService fileOpsService;
    TSystem testSystemSSH;
    TSystem testSystemS3;
    // mocking out the services
    private final SystemsClient systemsClient = Mockito.mock(SystemsClient.class);

    private ITestFileOpsService() {
        ServiceLocator locator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
//        ServiceLocatorUtilities.addClasses(locator, SSHDataClient.class);
        ServiceLocatorUtilities.bind(locator, new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(10)).to(SSHConnectionCache.class);
                bind(RemoteDataClientFactory.class).to(IRemoteDataClientFactory.class);
            }
        });
        locator.inject(this);
        fileOpsService = locator.getService(FileOpsService.class);
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystemSSH = new TSystem();
        testSystemSSH.setAccessCredential(creds);
        testSystemSSH.setHost("localhost");
        testSystemSSH.setPort(2222);
        testSystemSSH.setRootDir("/home/testuser/");
        testSystemSSH.setName("testSystem");
        testSystemSSH.setEffectiveUserId("testuser");
        testSystemSSH.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PASSWORD);
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystemSSH.setTransferMethods(transferMechs);

        Credential creds2 = new Credential();
        creds2.setAccessKey("user");
        creds2.setAccessSecret("password");
        testSystemS3 = new TSystem();
        testSystemS3.setHost("http://localhost");
        testSystemS3.setPort(9000);
        testSystemS3.setBucketName("test");
        testSystemS3.setName("testSystem");
        testSystemS3.setAccessCredential(creds2);
        testSystemS3.setRootDir("/");
        List<TSystem.TransferMethodsEnum> transferMechs2 = new ArrayList<>();
        transferMechs2.add(TSystem.TransferMethodsEnum.S3);
        testSystemS3.setTransferMethods(transferMechs2);
    }

    @DataProvider(name="testSystems")
    public Object[] testSystemsDataProvider () {
        return new TSystem[]{
                testSystemSSH,
                testSystemS3
        };
    }

    @BeforeTest()
    public void setUp() {

    }

    @AfterTest()
    public void tearDown() throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystemSSH);
        fileOpsService.delete("/");
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystemS3);
        fileOpsService = new FileOpsService(testSystemS3);
        fileOpsService.delete("/");
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDelete(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete("/test.txt");
        Assert.assertThrows(NotFoundException.class, ()-> {
            FileOpsService fos = new FileOpsService(testSystem);
            fos.ls("/test.txt");
        });
    }

    @Test(dataProvider = "testSystems")
    public void testInsertAndDeleteNested(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService;
        fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("a/b/c/test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/a/b/c/test.txt");
        Assert.assertEquals(listing.size(), 1);
        fileOpsService.delete("/a/b/");
        Assert.assertThrows(NotFoundException.class, ()-> {
            FileOpsService fos = new FileOpsService(testSystem);
            fos.ls("/a/b/c/test.txt");
        });
    }



    @Test(dataProvider = "testSystems")
    public void testInsertAndGet(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        InputStream out = fileOpsService.getStream("test.txt");
        Assert.assertEquals(out.readAllBytes().length,10* 1024);
        out.close();
    }


    @Test(dataProvider = "testSystems")
    public void testListing(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(10*1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/test.txt");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertTrue(listing.get(0).getName().equals("test.txt"));

    }

    @Test(dataProvider = "testSystems")
    public void testInsertLargeFile(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile(100 * 1000 * 1024);
        fileOpsService.insert("test.txt", in);
        List<FileInfo> listing = fileOpsService.ls("/test.txt");
        Assert.assertTrue(listing.size() == 1);
        Assert.assertTrue(listing.get(0).getName().equals("test.txt"));
        Assert.assertEquals(listing.get(0).getSize(), 100 * 1000 * 1024L);
    }

    @Test(dataProvider = "testSystems")
    public void testGetBytesByRange(TSystem testSystem) throws Exception {
        when(systemsClient.getSystemByName(any(String.class))).thenReturn(testSystem);
        FileOpsService fileOpsService = new FileOpsService(testSystem);
        InputStream in = Utils.makeFakeFile( 1000 * 1024);
        fileOpsService.insert("test.txt", in);
        InputStream result = fileOpsService.getBytes("test.txt", 0, 1000);
        Assert.assertEquals(result.readAllBytes().length, 1000);
    }

}
