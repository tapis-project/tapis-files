package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionHolder;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;

@Service
@Named
public class RemoteDataClientFactory implements IRemoteDataClientFactory {
    private final SSHConnectionCache sshConnectionCache;

    @Inject
    public RemoteDataClientFactory(SSHConnectionCache sshConnectionCache){
        this.sshConnectionCache = sshConnectionCache;
    }


    @Override
    public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                                 @NotNull TapisSystem system, @NotNull String username) throws IOException {

        if (SystemTypeEnum.LINUX.equals(system.getSystemType())) {
            SSHConnectionHolder holder = getSSHConnection(system, username);
            return new SSHDataClient(oboTenant, oboUser, system, holder);
        } else if (SystemTypeEnum.S3.equals(system.getSystemType())) {
            return new S3DataClient(oboTenant, oboUser, system);
        } else {
            throw new IOException(Utils.getMsg("FILES_CLIENT_PROTOCOL_INVALID", oboTenant, oboUser, system.getId(),
                system.getSystemType()));
        }
    }

    private SSHConnectionHolder getSSHConnection(TapisSystem system, String username) throws IOException {
        SSHConnectionHolder holder = sshConnectionCache.getConnection(system, username);
        return holder;
    }

}
