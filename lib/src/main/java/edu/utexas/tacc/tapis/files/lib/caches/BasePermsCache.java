package edu.utexas.tacc.tapis.files.lib.caches;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.TenantManager;

import javax.inject.Inject;

public class BasePermsCache {
    private static final String svcUserName = TapisConstants.SERVICE_NAME_FILES;
    private String siteId = null;
    private String svcTenantName = null;

    @Inject
    private ServiceClients serviceClients;

    /**
     * Get Security Kernel client
     * Need to use serviceClients.getClient() every time because it checks for expired service jwt token and
     *   refreshes it as needed.
     * Files service always calls SK as itself.
     * @return SK client
     * @throws TapisClientException - for Tapis related exceptions
     */
    protected SKClient getSKClient() throws TapisClientException
    {
        // Init if necessary
        if (siteId == null)
        {
            siteId = RuntimeSettings.get().getSiteId();
            svcTenantName = TenantManager.getInstance().getSiteAdminTenantId(siteId);
        }
        try { return serviceClients.getClient(svcUserName, svcTenantName, SKClient.class); }
        catch (Exception e)
        {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, svcTenantName, svcUserName);
            throw new TapisClientException(msg, e);
        }
    }
}
