package edu.utexas.tacc.tapis.files.lib.cache;

import com.google.common.cache.CacheLoader;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

import javax.validation.constraints.NotNull;
import java.io.IOException;

public class SSHConnectionCacheLoader extends CacheLoader<SSHConnectionCacheKey, SSHConnection> {

    @Override
    public SSHConnection load(@NotNull SSHConnectionCacheKey key) throws IOException, IllegalArgumentException {
        TSystem system = key.getSystem();
        String username = key.getUsername();
        int port;
        SSHConnection sshConnection;
        TSystem.DefaultAccessMethodEnum accessMethodEnum = system.getDefaultAccessMethod();
        //
        accessMethodEnum = accessMethodEnum == null ? TSystem.DefaultAccessMethodEnum.PASSWORD : accessMethodEnum;
        switch (accessMethodEnum) {
            case PASSWORD:
                String password = system.getAccessCredential().getPassword();
                port = system.getPort();
                if (port <= 0) port = 22;
                sshConnection = new SSHConnection(
                        system.getHost(),
                        port,
                        username,
                        password);
                sshConnection.initSession();
                return sshConnection;
            case PKI_KEYS:
                String pubKey = system.getAccessCredential().getPublicKey();
                String privateKey = system.getAccessCredential().getPrivateKey();
                port = system.getPort();
                if (port <= 0) port = 22;
                sshConnection = new SSHConnection(system.getHost(), username, port, pubKey, privateKey);
                sshConnection.initSession();
                return sshConnection;
            default:
                String msg = String.format("Access method of %s is not valid.", accessMethodEnum.getValue());
                throw new IllegalArgumentException(msg);

        }
    }
}
