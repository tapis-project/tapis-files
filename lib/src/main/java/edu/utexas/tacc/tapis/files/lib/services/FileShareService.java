package edu.utexas.tacc.tapis.files.lib.services;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.UserShareInfo;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;
import edu.utexas.tacc.tapis.security.client.model.SKShareDeleteShareParms;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
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
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
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
 * Notes:
 *   - Path provided will all be treated as relative to the system rootDir. See PathUtils.getRelativePath()
 *   - Path will be normalized
 *   - For sharing all paths will start with "/".
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
  private static final String OP_SHARE = "sharePath";
  private static final String OP_UNSHARE = "unSharePath";
  private static final String RESOURCE_TYPE = "file";
  private static final String svcUserName = TapisConstants.SERVICE_NAME_FILES;
  private static final Set<String> publicUserSet = Collections.singleton(SKClient.PUBLIC_GRANTEE); // "~public"

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  // TODO/TBD create a sharesCache?
  private final SystemsCache systemsCache;

  // Use HK2 to inject singletons
  @Inject
  private ServiceClients serviceClients;

  @Inject
  public FileShareService(SystemsCache sysCache)
  {
    systemsCache = sysCache;
  }

  private String siteId = null;
  private String svcTenantName = null;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

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
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    String relPathStr = relativePath.toString();
    // Add "/" to front of relative path, all sharing entries should look like absolute paths.
    String pathStr = String.format("/%s", relPathStr);

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(RESOURCE_TYPE);
    skParms.setResourceId1(systemId);

    ShareInfo shareInfo;
    SkShareList skShares;
    boolean isPublic;
    String isPublicPath = null;
    var userSet = new HashSet<String>();
    var userShareInfoSet = new HashSet<UserShareInfo>();

    // Catch client exceptions thrown by SK calls and convert them to WebApplicationException
    try
    {
      // First determine if path is publicly shared. Search for share on sys+path to grantee ~public
      // First check the specific path passed in
      skParms.setResourceId2(pathStr);
      skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
      skShares = getSKClient().getShares(skParms);
      // Set isPublic based on result.
      isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
      // If specific path is public then we know isPublicPath
      if (isPublic) isPublicPath = path;
      // If System is of type LINUX and specific path not public then one of the parent directories might be
      // So check all parent paths.
      if (SystemTypeEnum.LINUX.equals(sys.getSystemType()) && !isPublic)
      {
        // The method returns null if no public sharing found in parent paths.
        isPublicPath = checkForPublicInParentPaths(skParms, pathStr);
        isPublic = !StringUtils.isBlank(isPublicPath);
      }

      // Now get all the users with whom the path has been shared
      // First get users for the specific path
      skParms.setResourceId2(pathStr);
      skParms.setGrantee(null);
      skParms.setIncludePublicGrantees(false);
      skShares = getSKClient().getShares(skParms);
      if (skShares != null && skShares.getShares() != null)
      {
        for (SkShare skShare : skShares.getShares())
        {
          userSet.add(skShare.getGrantee());
          UserShareInfo usi = new UserShareInfo(skShare.getGrantee(), pathStr);
          userShareInfoSet.add(usi);
        }
      }

      // If system is of type LINUX then path may be shared with other users via a parent path
      // So process all parent paths, adding users to userSet and userShareInfoSet as we go along.
      checkForSharesInParentPaths(skParms, pathStr, userSet, userShareInfoSet);
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, "getShareInfo", systemId, path, relPathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
    shareInfo = new ShareInfo(isPublic, isPublicPath, userSet, userShareInfoSet);
    return shareInfo;
  }

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
    updateUserShares(rUser, OP_SHARE, systemId, path, userSet);
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
    updateUserShares(rUser, OP_UNSHARE, systemId, path, userSet);
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
    updateUserShares(rUser, OP_SHARE, systemId, path, publicUserSet);
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
    updateUserShares(rUser, OP_UNSHARE, systemId, path, publicUserSet);
  }

  // ************************************************************************
  // **************************  Private Methods  ***************************
  // ************************************************************************

  /*
   * Common routine to update share/unshare for a list of users.
   * Can be used to mark a path publicly shared with all users in tenant including "~public" in the set of users.
   */
  private void updateUserShares(ResourceRequestUser rUser, String opName, String systemId, String path,
                                Set<String> userSet)
          throws WebApplicationException
  {
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
    // Get path relative to system rootDir and protect against ../..
    Path relativePath = PathUtils.getRelativePath(path);
    String relPathStr = relativePath.toString();

    // Create request object needed for SK calls.
    ReqShareResource reqShareResource = null;
    SKShareDeleteShareParms deleteShareParms = null;
    switch (opName)
    {
      case OP_SHARE ->
      {
        reqShareResource = new ReqShareResource();
        reqShareResource.setResourceType(RESOURCE_TYPE);
        reqShareResource.setResourceId1(systemId);
        reqShareResource.setResourceId2(relPathStr);
        reqShareResource.setGrantor(rUser.getOboUserId());
        reqShareResource.setPrivilege(FileInfo.Permission.READ.name());
      }
      case OP_UNSHARE ->
      {
        deleteShareParms = new SKShareDeleteShareParms();
        deleteShareParms.setResourceType(RESOURCE_TYPE);
        deleteShareParms.setResourceId1(systemId);
        deleteShareParms.setResourceId2(relPathStr);
      }
    }

    // Catch client exceptions thrown by SK calls and convert them to WebApplicationException
    try
    {
      for (String userName : userSet)
      {
        switch (opName)
        {
          case OP_SHARE ->
          {
            reqShareResource.setGrantee(userName);
            getSKClient().shareResource(reqShareResource);
          }
          case OP_UNSHARE ->
          {
            deleteShareParms.setGrantee(userName);
            getSKClient().deleteShare(deleteShareParms);
          }
        }
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, opName, systemId, path, relPathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  /**
   * Check path to see if any of the parent paths is shared publicly.
   *
   * If System is of type LINUX and specific path not public then one of the parent directories might be
   * @param skParms Parameter set for calling SK with many attributes already set.
   * @param pathStr normalized path to check
   * @return path that allows public or null if no such path found.
   */
  private String checkForPublicInParentPaths(SKShareGetSharesParms skParms, String pathStr) throws TapisClientException
  {
    // If path is empty  or "/" then we are done.
    // We should never be given an empty string and if it is "/" then there are no parents and the calling routine
    //   will have already checked.
    if (StringUtils.isBlank(pathStr) || "/".equals(pathStr)) return null;
    Path path = Paths.get(pathStr);

    SkShareList skShares;
    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    // walk parent paths up to root
    Path parentPath = path.getParent();
    while (parentPath != null)
    {
      // Get shares for the path for grantee ~public
      String parentPathStr = parentPath.toString();
      skParms.setResourceId2(parentPathStr);
      skShares = getSKClient().getShares(skParms);
      // If any found then we are done
      if (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty())
      {
        return parentPathStr;
      }
      // Get the next parent path to check
      parentPath = parentPath.getParent();
    }
    return null;
  }

  /**
   * For given path collected all share information for all parent directories
   * Place results in provided Sets.
   * If system is of type LINUX then path may be shared with other users via a parent path
   * So process all parent paths, adding users to userSet and userShareInfoSet as we go along.
   *
   * @param skParms Parameter set for calling SK with many attributes already set.
   * @param pathStr normalized path to check
   * @param userSet add users found to this set
   * @param userShareInfoSet add additional share information to this set
   */
  private void checkForSharesInParentPaths(SKShareGetSharesParms skParms, String pathStr, Set<String> userSet,
                                             Set<UserShareInfo> userShareInfoSet)
          throws TapisClientException
  {
    // If path is empty  or "/" then we are done.
    // We should never be given an empty string and if it is "/" then there are no parents and the calling routine
    //   will have already processed it.
    if (StringUtils.isBlank(pathStr) || "/".equals(pathStr)) return;
    Path path = Paths.get(pathStr);

    SkShareList skShares;
    // Clear out any grantee previously set. We want all users.
    skParms.setGrantee(null);
    // walk parent paths up to root
    Path parentPath = path.getParent();
    while (parentPath != null)
    {
      // Get shares for the path
      String parentPathStr = parentPath.toString();
      skParms.setResourceId2(parentPathStr);
      //TODO
      skShares = getSKClient().getShares(skParms);
      if (skShares != null && skShares.getShares() != null)
      {
        for (SkShare skShare : skShares.getShares())
        {
          userSet.add(skShare.getGrantee());
          UserShareInfo usi = new UserShareInfo(skShare.getGrantee(), parentPathStr);
          userShareInfoSet.add(usi);
        }
      }

      // Get the next parent path to check
      parentPath = parentPath.getParent();
    }
  }

  /**
   * Get Security Kernel client with obo tenant and user set to the service tenant and user.
   * I.e. this is a client where the service calls SK as itself.
   * Need to use serviceClients.getClient() every time because it checks for expired service jwt token and
   *   refreshes it as needed.
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
    return getSKClient(svcUserName, svcTenantName);
  }

  /**
   * Get Security Kernel client with oboUser and oboTenant set as given.
   * Need to use serviceClients.getClient() every time because it checks for expired service jwt token and
   *   refreshes it as needed.
   * @param oboUser - obo user
   * @param oboTenant - obo tenant
   * @return SK client
   * @throws TapisException - for Tapis related exceptions
   */
  private SKClient getSKClient(String oboUser, String oboTenant) throws TapisClientException
  {
    try { return serviceClients.getClient(oboUser, oboTenant, SKClient.class); }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", TapisConstants.SERVICE_NAME_SECURITY, oboTenant, oboUser);
      throw new TapisClientException(msg, e);
    }
  }
}
