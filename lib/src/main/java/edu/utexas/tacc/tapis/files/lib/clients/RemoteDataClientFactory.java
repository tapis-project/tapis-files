package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.AuthnEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;

@Service
@Named
public class RemoteDataClientFactory implements IRemoteDataClientFactory {

    public RemoteDataClientFactory(){}

    @Override
    public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                                 @NotNull TapisSystem system, @NotNull String username) throws IOException {

        if (SystemTypeEnum.LINUX.equals(system.getSystemType())) {
            SSHConnection connection = getSSHConnection(system, username);
            return new SSHDataClient(oboTenant, oboUser, system, connection);
        } else if (SystemTypeEnum.S3.equals(system.getSystemType())) {
            return new S3DataClient(oboTenant, oboUser, system);
        } else {
            throw new IOException(Utils.getMsg("FILES_CLIENT_PROTOCOL_INVALID", oboTenant, oboUser, system.getId(),
                system.getSystemType()));
        }
    }

    private SSHConnection getSSHConnection(TapisSystem system, String username) throws IOException {
        SSHConnection sshConnection;
        int port;
        AuthnEnum accessMethodEnum = system.getDefaultAuthnMethod();
        // This should not be null, but if it is, we default to password.
        accessMethodEnum = accessMethodEnum == null ? AuthnEnum.PASSWORD : accessMethodEnum;
        try {
            if(accessMethodEnum.equals(AuthnEnum.PASSWORD)) {
                String password = system.getAuthnCredential().getPassword();
                port = system.getPort();
                if (port <= 0) port = 22;
                sshConnection = new SSHConnection(
                    system.getHost(),
                    port,
                    username,
                    password);
            } else if(accessMethodEnum.equals(AuthnEnum.PKI_KEYS)) {
                String pubKey = system.getAuthnCredential().getPublicKey();
                String privateKey = system.getAuthnCredential().getPrivateKey();
                port = system.getPort();
                if (port <= 0) port = 22;
                sshConnection = new SSHConnection(system.getHost(), username, port, pubKey, privateKey);
            } else {
                String msg = String.format("Access method of %s is not valid.", accessMethodEnum.getValue());
                throw new IllegalArgumentException(msg);
            }
            return sshConnection;
        } catch (TapisException ex) {
            throw new IOException(ex.getMessage(), ex);
        }
    }

}
