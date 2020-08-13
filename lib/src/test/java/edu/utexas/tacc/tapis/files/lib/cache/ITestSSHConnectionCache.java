package edu.utexas.tacc.tapis.files.lib.cache;

import com.jcraft.jsch.Channel;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.Credential;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class ITestSSHConnectionCache {

    TSystem testSystem;

    @BeforeTest
    public void setUp() {
        Credential creds = new Credential();
        creds.setAccessKey("testuser");
        creds.setPassword("password");
        testSystem = new TSystem();
        testSystem.setAccessCredential(creds);
        testSystem.setHost("localhost");
        testSystem.setPort(2222);
        testSystem.setRootDir("/home/testuser/");
        testSystem.setName("testSystem");
        testSystem.setEffectiveUserId("testuser");
        testSystem.setDefaultAccessMethod(TSystem.DefaultAccessMethodEnum.PASSWORD);
        List<TSystem.TransferMethodsEnum> transferMechs = new ArrayList<>();
        transferMechs.add(TSystem.TransferMethodsEnum.SFTP);
        testSystem.setTransferMethods(transferMechs);
    }


    @Test
    public void testCacheGet() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(100, TimeUnit.MILLISECONDS);
        SSHConnection test = cache.getConnection(testSystem, "testuser");
        Assert.assertNotNull(test);
    }

    @Test
    public void testCacheClosesSession() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(100, TimeUnit.MILLISECONDS);
        SSHConnection test = cache.getConnection(testSystem, "testuser");
        Thread.sleep(200);
        Assert.assertFalse(test.getSession().isConnected());
    }

    @Test
    public void testCacheKeepsSessionAlive() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(100, TimeUnit.MILLISECONDS);
        SSHConnection test = cache.getConnection(testSystem, "testuser");
        test.createChannel("sftp");
        Thread.sleep(200);
        Assert.assertTrue(test.getSession().isConnected());
        Assert.assertNotNull(cache.getCache().getIfPresent(new SSHConnectionCacheKey(testSystem, "testuser")));
    }

    @Test
    public void testCacheRemovesKeyAfterClose() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(100, TimeUnit.MILLISECONDS);
        SSHConnection test = cache.getConnection(testSystem, "testuser");
        Channel c = test.createChannel("sftp");
        Thread.sleep(200);
        Assert.assertTrue(test.getSession().isConnected());
        test.returnChannel(c);
        Thread.sleep(200);
        Assert.assertEquals(test.getChannelCount(), 0);
        Assert.assertFalse(test.getSession().isConnected());
        Assert.assertNull(cache.getCache().getIfPresent(new SSHConnectionCacheKey(testSystem, "testuser")));
    }

    @Test
    public void testMultipleThreads() throws Exception {
        SSHConnectionCache cache = new SSHConnectionCache(100, TimeUnit.MILLISECONDS);
        SSHConnection test = cache.getConnection(testSystem, "testuser");
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.submit(()-> {
            try {
                SSHConnection con = cache.getConnection(testSystem, "testuser");
                Channel channel = con.createChannel("stfp");
                Thread.sleep(200);
                //Should be the same instance as the one created above
                Assert.assertSame(con, test);
                con.returnChannel(channel);
            } catch (Exception e) {}
        });
        Thread.sleep(200);
        Assert.assertFalse(test.getSession().isConnected());
    }


}
