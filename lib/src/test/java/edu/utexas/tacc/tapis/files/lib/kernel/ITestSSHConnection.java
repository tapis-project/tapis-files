package edu.utexas.tacc.tapis.files.lib.kernel;

import com.jcraft.jsch.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@Test(groups = {"integration"}, enabled = false)
public class ITestSSHConnection {

    private static final Logger log = LoggerFactory.getLogger(ITestSSHConnection.class);

    String privateKey;
    String publicKey;

    public void testOpeningChannels() throws Exception {




        for (var i=0;i<1;i++) {
            SSHConnection connection = new SSHConnection(
                "localhost",
                2222,
                "testuser",
                "password"
            );

            for (var j = 0; j < 1; j++) {
                log.info("Opening channel {} in session {}", j, i);
                Channel c = connection.createChannel("sftp");
                try {
                    c.connect();
                } catch (Exception e) {
                    log.info("exited {}", c.getExitStatus());
                }
            }
        }

    }

}
