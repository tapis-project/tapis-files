package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.Utils;
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

public abstract class BaseFileOpsResource {

    @Inject
    SystemsCache systemsCache;

    @Inject
    RemoteDataClientFactory remoteDataClientFactory;

    @Inject
    ServiceContext serviceContext;

    private static final Logger log = LoggerFactory.getLogger(BaseFileOpsResource.class);
    private IRuntimeConfig settings = RuntimeSettings.get();

    protected IRemoteDataClient getClientForUserAndSystem(AuthenticatedUser authUser, TapisSystem system,
                                                          String effUserId)
            throws IOException
    {
      IRemoteDataClient client =
          remoteDataClientFactory.getRemoteDataClient(authUser.getOboTenantId(), authUser.getOboUser(), system, effUserId);
      return client;
    }

    protected IRemoteDataClient checkSystemAndGetClient(String systemId, AuthenticatedUser user, String path)
            throws NotFoundException, BadRequestException, IOException
    {
      TapisSystem system;
      try { system = systemsCache.getSystem(user.getOboTenantId(), systemId, user.getOboUser()); }
      catch (ServiceException ex)
      {
        throw new NotFoundException(Utils.getMsgAuth("FILES_SYS_NOTFOUND", user, systemId, path, ex.getMessage()));
      }
      Utils.checkEnabled(user, system);
      String effUser = StringUtils.isEmpty(system.getEffectiveUserId()) ? user.getOboUser() : system.getEffectiveUserId();
      return getClientForUserAndSystem(user, system, effUser);
    }
}
