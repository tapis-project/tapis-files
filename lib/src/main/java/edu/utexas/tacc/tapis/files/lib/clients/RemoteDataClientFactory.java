package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.cache.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.kernel.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem.TransferMethodsEnum;
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
    public IRemoteDataClient getRemoteDataClient(@NotNull TSystem system, @NotNull String username) throws IOException {

        List<TransferMethodsEnum> protocol = system.getTransferMethods();
        if (protocol.contains(TransferMethodsEnum.valueOf("SFTP"))) {
            SSHConnection sshConnection = sshConnectionCache.getConnection(system, username);
            return new SSHDataClient(system, sshConnection);
        } else if (protocol.contains(TransferMethodsEnum.valueOf("S3"))) {
            return new S3DataClient(system);
        } else {
            throw new IOException("Invalid or protocol not supported");
        }

    }
}
