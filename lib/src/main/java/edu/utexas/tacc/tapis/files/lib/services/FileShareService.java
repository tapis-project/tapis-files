package edu.utexas.tacc.tapis.files.lib.services;

import java.util.HashSet;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import java.util.Set;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.security.client.model.SKShareDeleteShareParms;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqShareResource;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShare;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShareList;
import edu.utexas.tacc.tapis.security.client.model.SKShareGetSharesParms;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

/*
 * Service level methods for File sharing operations. Support:
 *   - share/unshare path with users
 *   - share/unshare path with all users in a tenant (i.e. make public)
 *   - retrieve share info for a path
 *  - Paths provided will all be treated as relative to the system rootDir. Paths will be normalized. Please see
 *    PathUtils.java.
 *
 * Annotate as an hk2 Service so that default scope for Dependency Injection is singleton
 */
@Service
public class FileShareService
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // Tracing.
  private static final Logger log = LoggerFactory.getLogger(FileShareService.class);

  private static final IRuntimeConfig settings = RuntimeSettings.get();
  private static final String OP_SHARE_PATH = "sharePath";
  private static final String OP_UNSHARE_PATH = "unSharePath";
  private static final String OP_SHARE_PATH_PUBLIC = "sharePathPublic";
  private static final String OP_UNSHARE_PATH_PUBLIC = "unSharePathPublic";
  private static final String RESOURCE_TYPE = "file";
  private static final String svcUserName = TapisConstants.SERVICE_NAME_FILES;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // TODO/TBD create a sharesCache?
  private final FilePermsCache permsCache;
  private final SystemsCache systemsCache;

  // Use HK2 to inject singletons
  @Inject
  private ServiceClients serviceClients;

  @Inject
  public FileShareService(FilePermsCache permsCache, SystemsCache systemsCache)
  {
    this.permsCache = permsCache;
    this.systemsCache = systemsCache;
  }

  private String siteId = null;
  private String svcTenantName = null;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /**
   * Share a path with one or more users
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @param userSet - Set of users
   * @throws WebApplicationException - on error
   */
  public void sharePath(ResourceRequestUser rUser, String systemId, String path, Set<String> userSet)
          throws WebApplicationException
  {
    updateUserShares(rUser, OP_SHARE_PATH, systemId, path, userSet);
  }

  /**
   * UnShare a path with one or more users
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @param userSet - Set of users
   * @throws WebApplicationException - on error
   */
  public void unSharePath(ResourceRequestUser rUser, String systemId, String path, Set<String> userSet)
          throws WebApplicationException
  {
    updateUserShares(rUser, OP_UNSHARE_PATH, systemId, path, userSet);
  }

  /**
   * Share a path on a system publicly with all users in the tenant.
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @throws WebApplicationException - on error
   */
  public void sharePathPublic(ResourceRequestUser rUser, String systemId, String path)
          throws WebApplicationException
  {
    updatePublicShare(rUser, OP_SHARE_PATH_PUBLIC, systemId, path);
  }

  /**
   * Remove public access for a path on a system.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @throws WebApplicationException - on error
   */
  public void unSharePathPublic(ResourceRequestUser rUser, String systemId, String path)
          throws WebApplicationException
  {
    updatePublicShare(rUser, OP_UNSHARE_PATH_PUBLIC, systemId, path);
  }

  /**
   * Get share info for path
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @throws WebApplicationException - on error
   */
  public ShareInfo getShareInfo(ResourceRequestUser rUser, String systemId, String path)
          throws WebApplicationException
  {
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(RESOURCE_TYPE);
    skParms.setResourceId1(systemId);
    skParms.setResourceId2(path);

    ShareInfo shareInfo;
    SkShareList skShares;
    boolean isPublic = true;
    var userSet = new HashSet<String>();

    // Catch client exceptions and convert them to WebApplicationException
    try
    {
      // First determine if path is publicly shared. Search for share on sys+path to grantee ~public
      skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
      skShares = getSKClient().getShares(skParms);
      // Set isPublic based on result.
      isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());

      // Now get all the users with whom the path has been shared
      skParms.setGrantee(null);
      skParms.setIncludePublicGrantees(false);
      skShares = getSKClient().getShares(skParms);
      if (skShares != null && skShares.getShares() != null)
      {
        for (SkShare skShare : skShares.getShares()) { userSet.add(skShare.getGrantee()); }
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, "getShareInfo", systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }

    shareInfo = new ShareInfo(isPublic, userSet);
    return shareInfo;
  }

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /*
   * Common routine to update share/unshare for a list of users.
   */
  private void updateUserShares(ResourceRequestUser rUser, String opName, String systemId, String path,
                                Set<String> userSet)
          throws WebApplicationException
  {
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // Create request object needed for SK calls.
    ReqShareResource reqShareResource = null;
    SKShareDeleteShareParms deleteShareParms = null;
    switch (opName)
    {
      case OP_SHARE_PATH ->
      {
        reqShareResource = new ReqShareResource();
        reqShareResource.setResourceType(RESOURCE_TYPE);
        reqShareResource.setResourceId1(systemId);
        reqShareResource.setResourceId2(path);
        reqShareResource.setGrantor(rUser.getOboUserId());
        reqShareResource.setPrivilege(FileInfo.Permission.READ.name());
      }
      case OP_UNSHARE_PATH ->
      {
        deleteShareParms = new SKShareDeleteShareParms();
        deleteShareParms.setResourceType(RESOURCE_TYPE);
        deleteShareParms.setResourceId1(systemId);
        deleteShareParms.setResourceId2(path);
      }
    }

    // Catch client exceptions and convert them to WebApplicationException
    try
    {
      for (String userName : userSet)
      {

        switch (opName)
        {
          case OP_SHARE_PATH ->
          {
            reqShareResource.setGrantee(userName);
            getSKClient().shareResource(reqShareResource);
          }
          case OP_UNSHARE_PATH ->
          {
            deleteShareParms.setGrantee(userName);
            getSKClient().deleteShare(deleteShareParms);
          }
        }
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, opName, systemId, path, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  /*
   * Common routine to update public sharing.
   */
  private void updatePublicShare(ResourceRequestUser rUser, String opName, String systemId, String path)
          throws WebApplicationException
  {
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);

    // TODO
  }

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
      siteId = settings.getSiteId();
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
