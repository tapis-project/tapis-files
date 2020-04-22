package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

import javax.validation.constraints.NotNull;
import java.io.IOException;

public class SSHConnectionCacheLoader extends CacheLoader<SSHConnectionCacheKey, SSHConnection> {

    @Override
    public SSHConnection load(@NotNull SSHConnectionCacheKey key) throws IOException {
        TSystem system = key.getSystem();
        String username = key.getUsername();
        if (system.getDefaultAccessMethod() == TSystem.DefaultAccessMethodEnum.PASSWORD) {
            return new SSHConnection(
                    system.getHost(),
                    system.getPort(),
                    username,
                    system.getAccessCredential().getPassword()
            );
        } else {
            //TODO: PKI based session
            throw new IOException("Invalid Access Method");
        }
    }
}
