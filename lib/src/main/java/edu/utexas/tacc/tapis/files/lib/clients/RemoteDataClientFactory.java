package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Named;
import javax.validation.constraints.NotNull;
import org.jvnet.hk2.annotations.Service;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionHolder;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

@Service
@Named
public class RemoteDataClientFactory implements IRemoteDataClientFactory
{
  private final SSHConnectionCache sshConnectionCache;

  @Inject
  SystemsCache systemsCache;
  @Inject
  public RemoteDataClientFactory(SSHConnectionCache cache1)
  {
    sshConnectionCache = cache1;
  }

  /*
   * Convenience wrapper for backward compatibility.
   * Many callers do not need to pass in impersonationId and sharedCtxGrantor
   */
  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                               @NotNull TapisSystem system, @NotNull String effUserId)
          throws IOException
  {
    return getRemoteDataClient(oboTenant, oboUser, system, effUserId, null, null);
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
   * @param effUserId - User who is accessing system
   * @param impersonationId - needed to build key to invalidate system cache entry when connection fails.
   * @param sharedCtxGrantor - needed to build key to invalidate system cache entry when connection fails.
   * @return Remote data client
   * @throws IOException on error
   */
//  @Override
  public IRemoteDataClient getRemoteDataClient(@NotNull String oboTenant, @NotNull String oboUser,
                                               @NotNull TapisSystem system, @NotNull String effUserId,
                                               String impersonationId, String sharedCtxGrantor)
  throws IOException
  {
    if (SystemTypeEnum.LINUX.equals(system.getSystemType()))
    {
      // Attempt to get the connection. On error invalidate system and connection cache entries. Problem may get resolved soon.
      // For example, error could be invalid credentials or no route to host.
      // Probably no need to invalidate connection cache entry but do need to invalidate system cache entry because
      // that is where the credentials are coming from.
      SSHConnectionHolder holder;
      try { holder = sshConnectionCache.getConnection(system, system.getEffectiveUserId()); }
      catch (IOException e)
      {
        sshConnectionCache.invalidateConnection(system);
        systemsCache.invalidateEntry(oboTenant, oboUser, effUserId, impersonationId, sharedCtxGrantor);
        throw e;
      }
      return new SSHDataClient(oboTenant, oboUser, system, holder);
    }
    else if (SystemTypeEnum.S3.equals(system.getSystemType()))
    {
      return new S3DataClient(oboTenant, oboUser, system);
    }
    else if (SystemTypeEnum.IRODS.equals(system.getSystemType()))
    {
      return new IrodsDataClient(oboTenant, oboUser, system);
    }
    else
    {
      throw new IOException(LibUtils.getMsg("FILES_CLIENT_PROTOCOL_INVALID", oboTenant, oboUser, system.getId(),
              system.getSystemType()));
    }
  }
}
