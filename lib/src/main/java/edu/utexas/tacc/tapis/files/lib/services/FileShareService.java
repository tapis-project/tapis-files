package edu.utexas.tacc.tapis.files.lib.services;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import java.util.Set;

@Service
public class FileShareService
{
  private final FilePermsCache permsCache;
  private static final IRuntimeConfig settings = RuntimeSettings.get();
  private static final Logger log = LoggerFactory.getLogger(FileShareService.class);

  // PERMSPEC is "files:tenant:READ:systemId:path
  private static final String PERMSPEC = "files:%s:%s:%s:%s";

  @Inject
  private ServiceClients serviceClients;

  @Inject
  public FileShareService(FilePermsCache permsCache) {
    this.permsCache = permsCache;
  }


  private static final String svcUserName = TapisConstants.SERVICE_NAME_FILES;
  private String siteId = null;
  private String svcTenantName = null;

  /**
   * Share a path with one or more users
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param path - path on system relative to system rootDir
   * @param userSet - Set of users
   * @param rawData - Client provided text used to create the permissions list. Saved in update record.
   * @throws WebApplicationException - on error
   */
  public void sharePath(ResourceRequestUser rUser, TapisSystem system, String path, Set<String> userSet, String rawData)
          throws WebApplicationException
  {
    // TODO
    ?
  }

  /**
   * UnShare a path with one or more users
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param path - path on system relative to system rootDir
   * @param userSet - Set of users
   * @param rawData - Client provided text used to create the permissions list. Saved in update record.
   * @throws WebApplicationException - on error
   */
  public void unSharePath(ResourceRequestUser rUser, TapisSystem system, String path, Set<String> userSet, String rawData)
          throws WebApplicationException
  {
    // TODO
    ?
  }

  public ShareInfo getShareInfo(ResourceRequestUser rUser, TapisSystem system, String path)
          throws WebApplicationException
  {
    // TODO
    ?
    return null;
  }

//    public void grantPermission(String tenantId, String username, String systemId, String path, Permission perm) throws ServiceException {
//        try {
//            // This avoids ambiguous path issues with the SK. basically ensures that
//            // even if the path is dir/file1.txt the entry will be /dir/file1.txt
//            // Also removes any trailing slashes if present, needed for SK permissions checks
//            path = StringUtils.removeEnd(path, "/");
//            path = StringUtils.prependIfMissing(path, "/");
//            String permSpec = String.format(PERMSPEC, tenantId, perm, systemId, path);
//            getSKClient().grantUserPermission(tenantId, username, permSpec);
//        } catch (TapisClientException ex) {
//            String msg = LibUtils.getMsg("FILES_PERMC_ERR", tenantId, username, "grant", systemId, path, ex.getMessage());
//            throw new ServiceException(msg, ex);
//        }
//    }

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
