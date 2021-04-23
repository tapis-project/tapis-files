package edu.utexas.tacc.tapis.files.api.resources;

import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.clients.IRemoteDataClient;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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

    protected IRemoteDataClient getClientForUserAndSystem(AuthenticatedUser authUser, TapisSystem system, String effectiveUserId) throws IOException {
        IRemoteDataClient client =
                remoteDataClientFactory.getRemoteDataClient(authUser.getOboTenantId(), authUser.getOboUser(), system, effectiveUserId);
        return client;
    }
}
