package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.jcraft.jsch.Session;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

public class SSHConnectionCacheRemover implements RemovalListener<SSHConnectionCacheKey, SSHConnection> {

    private static final Logger log = LoggerFactory.getLogger(SSHConnectionCacheRemover.class);

    @Override
    public void onRemoval(@NotNull RemovalNotification removalNotification) {
        SSHConnection connection = (SSHConnection) removalNotification.getValue();
        log.info("removing connection to : " + connection.getSession().getHost());
        connection.getSession().disconnect();
    }
}
