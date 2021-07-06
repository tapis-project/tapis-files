package edu.utexas.tacc.tapis.files.lib.clients;


//import com.jcraft.jsch.Channel;
//import com.jcraft.jsch.ChannelSftp;
//import com.jcraft.jsch.SftpException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisSSHConnectionException;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

@Test
public class ITestSSHConnection {

    private static final Logger log = LoggerFactory.getLogger(ITestSSHConnection.class);
    public class RandomInputStream extends InputStream {

        private final long length;
        private long count;
        private final Random random;

        public RandomInputStream(long length) {
            this.length = length;
            this.random = new Random();
        }

        @Override
        public int read() throws IOException {
            if (count >= length) {
                return -1;
            }
            count++;
            return random.nextInt();
        }
    }

//    /**
//     * The test ssh container has a default of 10 open connections. After that, we should get the recoverable exception
//     * @throws Exception
//     */
//    @Test
//    public void testMaxSessions() throws Exception {
//        SSHConnection connection = new SSHConnection("localhost", 2222, "testuser", "password");
//        Assert.assertThrows(TapisSSHConnectionException.class, ()-> {
//            for (var j=0;j<20;j++) {
//                ChannelSftp channel = (ChannelSftp) connection.createChannel("sftp");
//                Vector results = channel.ls("/home/testuser/");
//            }
//        });
//    }
//
//    @Test
//    public void testNoPosixAuthorization() throws Exception {
//        SSHConnection connection = new SSHConnection("localhost", 2222, "testuser", "password");
//        Assert.assertThrows(SftpException.class, ()-> {
//            ChannelSftp channel = (ChannelSftp) connection.createChannel("sftp");
//            Vector results = channel.ls("/root/");
//        });
//    }

    @Test
    public void testNoHost() throws Exception {
        Assert.assertThrows(TapisException.class, () -> {
            SSHConnection connection = new SSHConnection("not-there", 22, "testuser", "password");
        });
    }

    @Test
    public void testBadAuth() throws Exception {
        Assert.assertThrows(TapisException.class, () -> {
            SSHConnection connection = new SSHConnection("localhost", 2222, "testuser", "NOT-the-password");
        });
    }

    @Test
    public void testWrongPort() throws Exception {
        Assert.assertThrows(TapisException.class, () -> {
            SSHConnection connection = new SSHConnection("localhost", 2224, "testuser", "password");
        });
    }

//    @Test
//    public void openChannelOnClosedSession() throws Exception {
//        SSHConnection connection = new SSHConnection("localhost", 2222, "testuser", "password");
//        connection.closeSession();
//        ChannelSftp channel = (ChannelSftp) connection.createChannel("sftp");
//        Vector results = channel.ls("/home/testuser/");
//        Assert.assertTrue(results.size()>0);
//    }
//
//    @Test
//    public void droppedSession() throws Exception {
//        SSHConnection connection = new SSHConnection("localhost", 2222, "testuser", "password");
//        ChannelSftp channel = (ChannelSftp) connection.createChannel("sftp");
//        Runnable background = ()-> {
//            try {
//                log.info("copying large file");
//                channel.put(new RandomInputStream(1000 * 1000 * 100), "/home/testuser/test.file");
//            } catch (Exception ex) {
//                Assert.assertTrue(ex instanceof SftpException);
//                Assert.assertTrue(ex.getMessage().contains("Pipe closed"));
//            }
//        };
//        Thread thread = new Thread(background);
//        thread.start();
//        // Give it a sec to get going
//        Thread.sleep(100);
//        connection.closeSession();
//    }

    // Disabled for now, the timeout takes the full 15 seconds at the moment which slows down the
    // integration tests.
    @Test(enabled = false)
    public void timeouts() throws Exception {
        try {
            SSHConnection connection = new SSHConnection("192.168.255.255", 22, "testuser", "password");
        } catch (TapisException ex) {
            Assert.assertTrue(ex.getMessage().contains("timeout"));
        }

    }
}
