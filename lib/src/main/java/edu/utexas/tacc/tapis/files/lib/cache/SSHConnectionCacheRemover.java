package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.jcraft.jsch.Session;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;

import javax.validation.constraints.NotNull;

public class SSHConnectionCacheRemover implements RemovalListener<SSHConnectionCacheKey, SSHConnection> {

    @Override
    public void onRemoval(@NotNull RemovalNotification removalNotification) {
        SSHConnection connection = (SSHConnection) removalNotification.getValue();
        connection.getSession().disconnect();
    }
}
