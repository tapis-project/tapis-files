package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionHolder;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jvnet.hk2.annotations.Service;

import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import java.io.IOException;

@Service
@Named
public class RemoteDataClientFactory implements IRemoteDataClientFactory
{
  private final SSHConnectionCache sshConnectionCache;

  // TODO/TBD
  @Inject
  private ServiceClients serviceClients;

  @Inject
  public RemoteDataClientFactory(SSHConnectionCache cache1) { sshConnectionCache = cache1; }

  /**
   * Return a remote data client from the cache
   *   given api tenant+user, system and user accessing system.
   * @param apiTenant - api tenant
   * @param apiUser - api user
   * @param system - Tapis System
   * @param effUserId - User who is accessing system
   * @return Remote data client
   * @throws IOException on error
   */
  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull String apiTenant, @NotNull String apiUser,
                                               @NotNull TapisSystem system, @NotNull String effUserId)
          throws IOException
  {
    if (SystemTypeEnum.LINUX.equals(system.getSystemType()))
    {
      SSHConnectionHolder holder = sshConnectionCache.getConnection(system, system.getEffectiveUserId());
      return new SSHDataClient(apiTenant, apiUser, system, holder);
    }
    else if (SystemTypeEnum.S3.equals(system.getSystemType()))
    {
      return new S3DataClient(apiTenant, apiUser, system);
    }
    else if (SystemTypeEnum.IRODS.equals(system.getSystemType()))
    {
      return new IrodsDataClient(apiTenant, apiUser, system);
    }
    else if (SystemTypeEnum.GLOBUS.equals(system.getSystemType()))
    {
//      return new GlobusDataClient(apiTenant, apiUser, system, null /*TODO/TBD serviceClients*/);
      return new GlobusDataClient(apiTenant, apiUser, system, serviceClients);
    }
    else
    {
      throw new IOException(LibUtils.getMsg("FILES_CLIENT_PROTOCOL_INVALID", apiTenant, apiUser, system.getId(),
              system.getSystemType()));
    }
  }
}
