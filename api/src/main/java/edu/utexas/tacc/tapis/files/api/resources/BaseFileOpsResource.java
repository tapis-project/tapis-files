package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.io.IOException;

public abstract class BaseFileOpsResource
{
  @Inject
  SystemsCache systemsCache;

  @Inject
  SystemsCacheNoAuth systemsCacheNoAuth;

  @Inject
  RemoteDataClientFactory remoteDataClientFactory;

  @Inject
  ServiceContext serviceContext;

  private static final Logger log = LoggerFactory.getLogger(BaseFileOpsResource.class);
  private IRuntimeConfig settings = RuntimeSettings.get();

  /**
   * Get a remote data client from the cache
   *
   * @param authUser
   * @param system
   * @param effUserId
   * @return
   * @throws IOException
   */
  protected IRemoteDataClient getClientForUserAndSystem(AuthenticatedUser authUser, TapisSystem system,
                                                        String effUserId)
          throws IOException
  {
    return remoteDataClientFactory.getRemoteDataClient(authUser.getOboTenantId(), authUser.getOboUser(), system, effUserId);
  }

  /**
   * Retrieve remote data client from the cache.
   * First fetch Tapis system (which ensures it exists)
   * Provided path is used only for logging.
   *
   * TODO/TBD: See github issue https://github.com/tapis-project/tapis-files/issues/39
   *   seems that there may be a race condition. The stop sets the session to null but the
   *   cached connection is still available? See SSHConnectionHolder, SSHConnectionCache
   * TODO
   *   Seems that problem is the connection can be shut down after retrieving it here
   *     and then using it to do something like in: SSHDataClient.mkdir()
   *
   * @param systemId - Tapis System
   * @param user - Authenticated user (obo user)
   * @param path - path, if any, for logging purposes only.
   * @return Remote data client from the cache
   * @throws NotFoundException system not found
   * @throws BadRequestException system not enabled
   * @throws IOException other error
   */
  protected IRemoteDataClient checkSystemAndGetClient(String systemId, AuthenticatedUser user, String path)
          throws NotFoundException, BadRequestException, IOException
  {
    TapisSystem system;
    try { system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser()); }
    catch (ServiceException ex)
    {
      throw new NotFoundException(LibUtils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId, path, ex.getMessage()));
    }
    LibUtils.checkEnabled(user, system);
    String effUser = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
    return getClientForUserAndSystem(user, system, effUser);
  }
}
