package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.utils.Utils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.ssh.apache.system.TapisSSH;
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
        try {
            TapisSSH ssh = new TapisSSH(system);
            return ssh.getConnection();
        } catch (TapisException ex) {
            throw new IOException(ex.getMessage(), ex);
        }
    }

}
