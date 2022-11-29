package edu.utexas.tacc.tapis.files.api.resources;

import java.io.IOException;
import javax.inject.Inject;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

public abstract class BaseFileOpsResource
{
  @Inject
  SystemsCache systemsCacheWithAuth;
  @Inject
  SystemsCacheNoAuth systemsCacheNoAuth;
  @Inject
  RemoteDataClientFactory remoteDataClientFactory;

  /**
   * Get a remote data client from the cache
   *
   * @param rUser Authenticated user
   * @param system system for connection
   * @param effUserId effective user to be used when connecting to host
   * @return a remote data client
   * @throws IOException on error
   */
  protected IRemoteDataClient getClientForUserAndSystem(ResourceRequestUser rUser, TapisSystem system, String effUserId)
          throws IOException
  {
    return remoteDataClientFactory.getRemoteDataClient(rUser.getOboTenantId(), rUser.getOboUserId(), system, effUserId);
  }
}
