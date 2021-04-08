package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnection;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TransferMethodEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.ResultSystem;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;

@Service
@Named
public class RemoteDataClientFactory implements IRemoteDataClientFactory {

    private final SSHConnectionCache sshConnectionCache;

    @Inject
    public RemoteDataClientFactory(SSHConnectionCache sshConnectionCache) {
        this.sshConnectionCache = sshConnectionCache;
    }

    @Override
    public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                                 @NotNull ResultSystem system, @NotNull String username) throws IOException {

        List<TransferMethodEnum> protocols = system.getTransferMethods();
        if (protocols == null || protocols.isEmpty())
          throw new IOException(Utils.getMsg("FILES_CLIENT_PROTOCOL_NULL", oboTenant, oboUser, system.getId()));
        if (protocols.contains(TransferMethodEnum.valueOf("SFTP"))) {
            SSHConnection sshConnection = sshConnectionCache.getConnection(system, username);
            return new SSHDataClient(oboTenant, oboUser, system, sshConnection);
        } else if (protocols.contains(TransferMethodEnum.valueOf("S3"))) {
            return new S3DataClient(oboTenant, oboUser, system);
        } else {
            throw new IOException(Utils.getMsg("FILES_CLIENT_PROTOCOL_INVALID", oboTenant, oboUser, system.getId(),
                                                   Utils.getTransferMethodsAsString(protocols)));
        }

    }
}
