package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import org.jvnet.hk2.annotations.Service;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

@Service
@Named
public class RemoteDataClientFactory implements IRemoteDataClientFactory
{
  @Inject
  private ServiceClients serviceClients;
  @Inject
  SystemsCache systemsCache;

  /*
   * Convenience wrapper for backward compatibility.
   * Many callers do not need to pass in impersonationId and sharedCtxGrantor
   */
  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                               @NotNull TapisSystem system)
          throws IOException
  {
    return getRemoteDataClient(oboTenant, oboUser, system, null, null);
  }

  /**
   * Return a remote data client from the cache given obo tenant+user, system and user accessing system.
   * For a LINUX system if ssh connection fails then the system cache entry is invalidated.
   * Incoming arguments impersonationId and sharedCtxGrantor are only relevant for LINUX systems
   *   and only used to build the key to invalidate the system cache entry.
   *
   * @param oboTenant - api tenant
   * @param oboUser - api user
   * @param system - Tapis System
   * @param impersonationId - needed to build key to invalidate system cache entry when connection fails.
   * @param sharedCtxGrantor - needed to build key to invalidate system cache entry when connection fails.
   * @return Remote data client
   * @throws IOException on error
   */
//  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                               @NotNull TapisSystem system, String impersonationId,
                                               String sharedCtxGrantor)
  throws IOException
  {
    if (SystemTypeEnum.LINUX.equals(system.getSystemType()))
    {
      return new SSHDataClient(oboTenant, oboUser, system, systemsCache, impersonationId, sharedCtxGrantor);
    }
    else if (SystemTypeEnum.S3.equals(system.getSystemType()))
    {
      return new S3DataClient(oboTenant, oboUser, system);
    }
    else if (SystemTypeEnum.IRODS.equals(system.getSystemType()))
    {
      return new IrodsDataClient(oboTenant, oboUser, system);
    }
    else if (SystemTypeEnum.GLOBUS.equals(system.getSystemType()))
    {
      return new GlobusDataClient(oboTenant, oboUser, system, serviceClients);
    }
    else
    {
      throw new IOException(LibUtils.getMsg("FILES_CLIENT_PROTOCOL_INVALID", oboTenant, oboUser, system.getId(),
              system.getSystemType()));
    }
  }
}
