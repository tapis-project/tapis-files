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

import edu.utexas.tacc.tapis.security.client.model.SKShareDeleteShareParms;
import edu.utexas.tacc.tapis.systems.client.gen.model.SystemTypeEnum;
import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.ReqShareResource;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShare;
import edu.utexas.tacc.tapis.security.client.gen.model.SkShareList;
import edu.utexas.tacc.tapis.security.client.model.SKShareGetSharesParms;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.models.UserShareInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.lib.utils.PathUtils;

/*
 * Service level methods for File sharing operations.
 * Support:
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

  private static final String OP_SHARE = "sharePath";
  private static final String OP_UNSHARE = "unSharePath";
  private static final String RESOURCE_TYPE = "file";
  private static final Set<String> publicUserSet = Collections.singleton(SKClient.PUBLIC_GRANTEE); // "~public"

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

// TODO/TBD create a sharesCache?
//  private final SharesCache sharesCache;

  // Use HK2 to inject singletons
  @Inject
  private SystemsCache systemsCache;
  @Inject
  private ServiceClients serviceClients;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  /*
   * Check to see if a path is shared with a user.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param path - path on system relative to system rootDir
   * @return true if shared
   * @throws TapisClientExcepion - error calling SK
   */
  public boolean isSharedWithUser(ResourceRequestUser rUser, TapisSystem system, String path, String user)
          throws TapisClientException
  {
    // Check arguments. Return false if any are not valid.
    if (rUser == null || system == null || StringUtils.isBlank(user)) return false;

    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();
    String systemId = system.getId();

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(RESOURCE_TYPE);
    skParms.setResourceId1(systemId);

    SkShareList skShares;
    boolean isPublic;
    String isPublicPath;

//    // Catch client exceptions thrown by SK calls and convert them to WebApplicationException
//    try
//    {
      // First determine if path is publicly shared. Search for share on sys+path to grantee ~public
      // First check the specific path passed in
      skParms.setResourceId2(pathStr);
      skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);
      // Set isPublic based on result.
      isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
      // If System is of type LINUX and specific path not public then one of the parent directories might be
      // So check all parent paths.
      if (SystemTypeEnum.LINUX.equals(system.getSystemType()) && !isPublic)
      {
        // The method returns null if no public sharing found in parent paths.
        isPublicPath = checkForPublicInParentPaths(oboUser, oboTenant, skParms, pathStr);
        isPublic = !StringUtils.isBlank(isPublicPath);
      }

      // ============================================
      // If publicly shared then return true
      // ============================================
      if (isPublic) return true;

      // Now check to see if given path is shared directly with given user.
      skParms.setResourceId2(pathStr);
      skParms.setGrantee(user);
      skParms.setIncludePublicGrantees(false);
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);

      // ====================================================
      // If specific path shared with user then return true
      // ====================================================
      if (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty()) return true;

      // If system is of type LINUX then path may be shared with a user via a parent path
      if (SystemTypeEnum.LINUX.equals(system.getSystemType()))
      {
        // Check parent paths. This is the final check, so simply return
        return checkForGranteeInParentPaths(rUser.getOboUserId(), rUser.getOboTenantId(), skParms, pathStr);
      }
//    }
//    catch (TapisClientException e)
//    {
//      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, "getShareInfo", systemId, path, pathStr, e.getMessage());
//      log.error(msg, e);
//      throw new WebApplicationException(msg, e);
//    }
//    shareInfo = new ShareInfo(isPublic, isPublicPath, userSet, userShareInfoSet);
//    return shareInfo;

    // No shares found, path not shared, return false.
    return false;
  }

  // =================================================================================
  //  Basic CRUD methods: get, share, unshare, sharePublic, unsharePublic, removeAll
  // =================================================================================

  /**
   * Get share info for path
   * Sharing means grantees effectively have READ permission on the path.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @return shareInfo containing all share information
   * @throws WebApplicationException - on error
   */
  public ShareInfo getShareInfo(ResourceRequestUser rUser, String systemId, String path)
          throws WebApplicationException
  {
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();

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
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);
      // Set isPublic based on result.
      isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
      // If specific path is public then we know isPublicPath
      if (isPublic) isPublicPath = path;
      // If System is of type LINUX and specific path not public then one of the parent directories might be
      // So check all parent paths.
      if (SystemTypeEnum.LINUX.equals(sys.getSystemType()) && !isPublic)
      {
        // The method returns null if no public sharing found in parent paths.
        isPublicPath = checkForPublicInParentPaths(oboUser, oboTenant, skParms, pathStr);
        isPublic = !StringUtils.isBlank(isPublicPath);
      }

      // Now get all the users with whom the path has been shared
      // First get users for the specific path
      skParms.setResourceId2(pathStr);
      skParms.setGrantee(null);
      skParms.setIncludePublicGrantees(false);
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);
      if (skShares != null && skShares.getShares() != null)
      {
        for (SkShare skShare : skShares.getShares())
        {
          userSet.add(skShare.getGrantee());
          UserShareInfo usi = new UserShareInfo(skShare.getGrantee(), pathStr, skShare.getGrantor());
          userShareInfoSet.add(usi);
        }
      }

      // If system is of type LINUX then path may be shared with other users via a parent path
      if (SystemTypeEnum.LINUX.equals(sys.getSystemType()))
      {
        // So process all parent paths, adding users to userSet and userShareInfoSet as we go along.
        checkForSharesInParentPaths(rUser.getOboUserId(), rUser.getOboTenantId(), skParms, pathStr, userSet, userShareInfoSet);
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, "getShareInfo", systemId, path, pathStr, e.getMessage());
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
   * Remove all shares for a path
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @throws WebApplicationException - on error
   */
  public void removeAllSharesForPath(ResourceRequestUser rUser, String systemId, String path)
          throws WebApplicationException
  {
    // Make sure the Tapis System exists and is enabled
    LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();

    // Create request objects needed for SK calls.
    var skGetParms = new SKShareGetSharesParms();
    skGetParms.setResourceType(RESOURCE_TYPE);
    skGetParms.setResourceId1(systemId);
    skGetParms.setResourceId2(pathStr);

    var skDeleteParms = new SKShareDeleteShareParms();
    skDeleteParms.setResourceType(RESOURCE_TYPE);
    skDeleteParms.setResourceId1(systemId);
    skDeleteParms.setResourceId2(pathStr);
    skDeleteParms.setPrivilege(FileInfo.Permission.READ.name());

    // We will be calling SK which can throw TapisClientException. Handle it by converting to WebAppException
    try
    {
      // Remove public sharing
      skDeleteParms.setGrantee(SKClient.PUBLIC_GRANTEE);
      getSKClient(oboUser, oboTenant).deleteShare(skDeleteParms);

      // Get all users
      SkShareList shareList = getSKClient(oboUser, oboTenant).getShares(skGetParms);
      var userSet = new HashSet<String>();
      if (shareList != null && shareList.getShares() != null)
      {
        for (SkShare skShare : shareList.getShares()) { userSet.add(skShare.getGrantee()); }
      }

      // For each user remove the share
      for (String userName : userSet)
      {
        skDeleteParms.setGrantee(userName);
        getSKClient(oboUser, oboTenant).deleteShare(skDeleteParms);
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, OP_UNSHARE, systemId, path, pathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
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
    LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();

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
        reqShareResource.setResourceId2(pathStr);
        reqShareResource.setGrantor(oboUser);
        reqShareResource.setPrivilege(FileInfo.Permission.READ.name());
      }
      case OP_UNSHARE ->
      {
        deleteShareParms = new SKShareDeleteShareParms();
        deleteShareParms.setResourceType(RESOURCE_TYPE);
        deleteShareParms.setResourceId1(systemId);
        deleteShareParms.setResourceId2(pathStr);
        deleteShareParms.setPrivilege(FileInfo.Permission.READ.name());
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
            getSKClient(oboUser, oboTenant).shareResource(reqShareResource);
          }
          case OP_UNSHARE ->
          {
            deleteShareParms.setGrantee(userName);
            getSKClient(oboUser, oboTenant).deleteShare(deleteShareParms);
          }
        }
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_ERR", rUser, opName, systemId, path, pathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
  }

  /**
   * Check path to see if a parent path is shared publicly.
   * If System is of type LINUX and specific path not public then one of the parent directories might be
   *
   * @param skParms Parameter set for calling SK with many attributes already set.
   * @param pathStr normalized path to check
   * @return path that allows public or null if no such path found.
   */
  private String checkForPublicInParentPaths(String oboUser, String oboTenant, SKShareGetSharesParms skParms,
                                             String pathStr)
          throws TapisClientException
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
      // Note: leave getSKClient in the loop since the svc jwt might expire.
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);
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
   * For given path collect all share information for all parent directories
   * Place results in provided Sets.
   * If system is of type LINUX then path may be shared with other users via a parent path
   * So process all parent paths, adding users to userSet and userShareInfoSet as we go along.
   *
   * @param skParms Parameter set for calling SK with many attributes already set.
   * @param pathStr normalized path to check
   * @param userSet add users found to this set
   * @param userShareInfoSet add additional share information to this set
   */
  private void checkForSharesInParentPaths(String oboUser, String oboTenant, SKShareGetSharesParms skParms,
                                           String pathStr, Set<String> userSet, Set<UserShareInfo> userShareInfoSet)
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
      // Note: leave getSKClient in the loop since the svc jwt might expire.
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);
      if (skShares != null && skShares.getShares() != null)
      {
        for (SkShare skShare : skShares.getShares())
        {
          userSet.add(skShare.getGrantee());
          UserShareInfo usi = new UserShareInfo(skShare.getGrantee(), parentPathStr, skShare.getGrantor());
          userShareInfoSet.add(usi);
        }
      }
      // Get the next parent path to check
      parentPath = parentPath.getParent();
    }
  }

  /**
   * If system is of type LINUX then path may be shared with other users via a parent path
   * So process all parent paths.
   * The grantee must have already been set in skParms
   *
   * @param skParms Parameter set for calling SK with many attributes already set.
   * @param pathStr normalized path to check
   * @return true if a parent path is shared with Grantee contained in skParms
   */
  private boolean checkForGranteeInParentPaths(String oboUser, String oboTenant, SKShareGetSharesParms skParms,
                                               String pathStr)
          throws TapisClientException
  {
    // If path is empty or "/" then we are done.
    // We should never be given an empty string and if it is "/" then there are no parents and the calling routine
    //   will have already processed it.
    if (StringUtils.isBlank(pathStr) || "/".equals(pathStr)) return false;

    Path path = Paths.get(pathStr);
    SkShareList skShares;
    // walk parent paths up to root
    Path parentPath = path.getParent();
    while (parentPath != null)
    {
      // Get shares for the path
      String parentPathStr = parentPath.toString();
      skParms.setResourceId2(parentPathStr);
      skShares = getSKClient(oboUser, oboTenant).getShares(skParms);
      // ====================================================
      // If share found then return true
      // ====================================================
      if (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty()) return true;
      // Get the next parent path to check
      parentPath = parentPath.getParent();
    }
    // No shares found
    return false;
  }

  /**
   * Get Security Kernel client with oboUser and oboTenant set as given.
   * Do not cache.
   * Always use serviceClients.getClient() because it checks for expired service jwt token and refreshes as needed.
   * @param oboUser - obo user
   * @param oboTenant - obo tenant
   * @return SK client
   * @throws TapisClientException - for Tapis related exceptions
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
