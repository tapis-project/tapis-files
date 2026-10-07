package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.transfers.FilesAppContext;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class SkUtils {

    @Inject
    private ServiceClients serviceClients;
    private static final Logger log = LoggerFactory.getLogger(SkUtils.class);

    /**
     * Get Security Kernel client
     * Need to use serviceClients.getClient() every time because it checks for expired service jwt token and
     *   refreshes it as needed.
     * Files service always calls SK as itself.
     * @return SK client
     * @throws TapisClientException - for Tapis related exceptions
     */
    private SKClient getSKClient() throws TapisClientException
    {
        var siteAdminTenantId = FilesAppContext.getSiteAdminTenantId();
        try { return serviceClients.getClient(TapisConstants.SERVICE_NAME_FILES, siteAdminTenantId, SKClient.class); }
        catch (Exception e)
        {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, siteAdminTenantId, TapisConstants.SERVICE_NAME_FILES);
            log.error(msg, e);
            throw new TapisClientException(msg, e);
        }
    }
}
