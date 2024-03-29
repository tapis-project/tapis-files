package edu.utexas.tacc.tapis.files.lib.services;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.WebApplicationException;
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
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo;
import edu.utexas.tacc.tapis.files.lib.models.ShareInfo;
import edu.utexas.tacc.tapis.files.lib.models.UserShareInfo;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;

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
 * NOTE: Paths stored in SK for permissions and shares always relative to rootDir and always start with /
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

  private static final String SERVICE_NAME = TapisConstants.SERVICE_NAME_FILES;
  private static final String OP_SHARE = "sharePath";
  private static final String OP_UNSHARE = "unSharePath";
  private static final String RESOURCE_TYPE = "file";
  private static final Set<String> publicUserSet = Collections.singleton(SKClient.PUBLIC_GRANTEE); // "~public"
  private static final String SK_WILDCARD = "%";

  private static final String svcUserName = TapisConstants.SERVICE_NAME_FILES;
  private String svcTenantName = null;

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

// TODO/TBD create a sharesCache?
//  private final SharesCache sharesCache;

  // Use HK2 to inject singletons
  @Inject
  FilePermsService permsService;
  @Inject
  private SystemsCache systemsCache;
  @Inject
  private ServiceClients serviceClients;

  private static String siteAdminTenantId;

  // ************************************************************************
  // *********************** Public Methods *********************************
  // ************************************************************************

  public static void setSiteAdminTenantId(String s) { siteAdminTenantId = s; }

  /*
   * Check to see if a path is shared with a user.
   *
   * NOTE: No permission checking
   * NOTE: We do not set grantor when checking.
   * NOTE: This method is not called by an api resource, so it does not throw WebApplicationException.
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

    log.debug(LibUtils.getMsgAuthR("FILES_AUTH_SHARE_CHECK", rUser, systemId, user, pathStr));

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(RESOURCE_TYPE);
    skParms.setTenant(system.getTenant());
    skParms.setResourceId1(systemId);

    SkShareList skShares;
    boolean isPublic;
    String isPublicPath = null;

    // First determine if path is publicly shared. Search for share on sys+path to grantee ~public
    // First check the specific path passed in
    skParms.setResourceId2(pathStr);
    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
    skShares = getSKClient().getShares(skParms);
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
    log.debug(LibUtils.getMsgAuthR("FILES_AUTH_SHARE_CHECK_PUB", rUser, systemId, user, pathStr, isPublic, isPublicPath));
    if (isPublic) return true;

    // Now check to see if given path is shared directly with given user.
    skParms.setResourceId2(pathStr);
    skParms.setGrantee(user);
    skParms.setIncludePublicGrantees(false);
    skShares = getSKClient().getShares(skParms);

    // ====================================================
    // If specific path shared with user then return true
    // ====================================================
    if (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty())
    {
      log.debug(LibUtils.getMsgAuthR("FILES_AUTH_SHARE_CHECK_USR", rUser, systemId, user, pathStr, true, null));
      return true;
    }

    // If system is of type LINUX then path may be shared with a user via a parent path
    if (SystemTypeEnum.LINUX.equals(system.getSystemType()))
    {
      // Check parent paths.
      String sharedParentPath = checkForGranteeInParentPaths(skParms, pathStr);
      boolean isShared = !StringUtils.isBlank(sharedParentPath);
      log.debug(LibUtils.getMsgAuthR("FILES_AUTH_SHARE_CHECK_USR", rUser, systemId, user, pathStr, isShared, sharedParentPath));
      if (isShared) return true;
    }

    // No shares found, path not shared, return false.
    return false;
  }



  /*
   * TODO: Retrieve specific share that allows user access to a path.
   * If no access allowed then return null.
   *
   * NOTE: This method is not called by an api resource, so it does not throw WebApplicationException.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - Tapis system
   * @param path - path on system relative to system rootDir
   * @return user share info object
   * @throws TapisClientExcepion - error calling SK
   */
  public UserShareInfo getShareInfoForPath(ResourceRequestUser rUser, TapisSystem system, String path, String user)
          throws TapisClientException
  {
    // Check arguments. Return false if any are not valid.
    if (rUser == null || system == null || StringUtils.isBlank(user)) return null;

    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();
    String systemId = system.getId();

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(RESOURCE_TYPE);
    skParms.setTenant(system.getTenant());
    skParms.setResourceId1(systemId);
//
//    SkShareList skShares;
//    boolean isPublic;
//    String isPublicPath;
//
//    // First determine if path is publicly shared. Search for share on sys+path to grantee ~public
//    // First check the specific path passed in
//    skParms.setResourceId2(pathStr);
//    skParms.setGrantee(SKClient.PUBLIC_GRANTEE);
//    skShares = getSKClient().getShares(skParms);
//    // Set isPublic based on result.
//    isPublic = (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty());
//    // If System is of type LINUX and specific path not public then one of the parent directories might be
//    // So check all parent paths.
//    if (SystemTypeEnum.LINUX.equals(system.getSystemType()) && !isPublic)
//    {
//      // The method returns null if no public sharing found in parent paths.
//      isPublicPath = checkForPublicInParentPaths(oboUser, oboTenant, skParms, pathStr);
//      isPublic = !StringUtils.isBlank(isPublicPath);
//    }
//
//    // ============================================
//    // If publicly shared then return true
//    // ============================================
//    if (isPublic) return true;
//
//    // Now check to see if given path is shared directly with given user.
//    skParms.setResourceId2(pathStr);
//    skParms.setGrantee(user);
//    skParms.setIncludePublicGrantees(false);
//    skShares = getSKClient().getShares(skParms);
//
//    // ====================================================
//    // If specific path shared with user then return true
//    // ====================================================
//    if (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty()) return true;
//
//    // If system is of type LINUX then path may be shared with a user via a parent path
//    if (SystemTypeEnum.LINUX.equals(system.getSystemType()))
//    {
//      // Check parent paths. This is the final check, so simply return
//      return checkForGranteeInParentPaths(rUser.getOboUserId(), rUser.getOboTenantId(), skParms, pathStr);
//    }
//
    // No shares found, path not shared, return null.
    return null;
  }

  // =================================================================================
  //  Basic CRUD methods: get, share, unshare, sharePublic, unsharePublic, removeAll
  // =================================================================================

  /**
   * Get share info for path
   * Sharing means grantees effectively have READ permission on the path.
   *
   * Requesting user must have READ permission.
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
    String opName = "getShareInfo";
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
    // Is the requester the owner of the system?
    boolean isOwner = rUser.getOboUserId().equals(sys.getOwner());

    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();

    // TODO
    // User must be system owner or have READ permission or share for path
    // TODO at some point need to support a way for a user to see which files are shared with them.
    //   but they probably should be limited to seeing only their own information, so they should not get
    //   the full ShareInfo details.
    //  So for now limit this method to only users who have READ permission.
//    try
//    {
//      if (!permsService.isPermitted(oboTenant, oboUser, systemId, pathStr, FileInfo.Permission.READ) &&
//            !isSharedWithUser(rUser, sys, pathStr, rUser.getOboUserId()))
//      {
//        String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", oboTenant, oboUser, systemId, pathStr, FileInfo.Permission.READ);
//        log.warn(msg);
//        throw new ForbiddenException(msg);
//
//      }
//      LibUtils.checkPermitted(permsService, oboTenant, oboUser, systemId, pathStr, FileInfo.Permission.READ);
//    }
//    catch (ServiceException ex)
//    {
//      log.error(msg, ex);
//      throw new WebApplicationException(LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, systemId, pathStr,
//              ex.getMessage()), ex);
//    }
    // User must be system owner or have READ permission
    try
    {
      // If not the owner check for READ permission
      if (!isOwner)
      {
        LibUtils.checkPermitted(permsService, oboTenant, oboUser, systemId, pathStr, FileInfo.Permission.READ);
      }
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_OPSCR_ERR", rUser, opName, systemId, pathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }

    // Create SKShareGetSharesParms needed for SK calls.
    var skParms = new SKShareGetSharesParms();
    skParms.setResourceType(RESOURCE_TYPE);
    skParms.setTenant(sys.getTenant());
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
        isPublicPath = checkForPublicInParentPaths(oboUser, oboTenant, skParms, pathStr);
        isPublic = !StringUtils.isBlank(isPublicPath);
      }

      // TODO/TBD: For each UserShareInfo that we record do we need to check if it exists because of public sharing
      //           and set field? See class UserShareInfo2.
      //           Seems like NO, here we are just collecting all sharing related info for the path (and parent paths).
      //           If path is public then ALL users can access it and of course we don't want to add UserShareInfo
      //           records for ALL users.
      // Now get all user-specific shares for the path
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
   * Only System owner may share file paths.
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

  /**
   * Remove all share access for a path on a system including public.
   * Must be system owner
   * Retrieve all shares and use deleteShareById.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @param recurse - if true include all sub-paths as well
   * @throws WebApplicationException - on error
   */
  public void removeAllSharesForPath(ResourceRequestUser rUser, String systemId, String path, boolean recurse)
          throws WebApplicationException
  {
    String opName = "removeAllSharesForPath";
    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();
    // Make sure the Tapis System exists and is enabled
    TapisSystem sys = LibUtils.getSystemIfEnabled(rUser, systemsCache, systemId);
    // Is the requester the owner of the system?
    boolean isOwner = rUser.getOboUserId().equals(sys.getOwner());

    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // User must be system owner
    if (!isOwner)
    {
      String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", oboTenant, oboUser, systemId, pathStr, opName);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }

    removeAllSharesForPathWithoutAuth(oboTenant, systemId, path, recurse);
  }

  /**
   * Remove all share access for a path on a system including public.
   * NOTE:  This will not check permissions!  It will just delete the
   * shares.  One use of this is when files are deleted, we need to
   * remove the shares for that file.  If you had permission to delete
   * the file, the shares should be removed regardless of permissions.
   * Retrieve all shares and use deleteShareById.
   *
   * @param tenant - The tenant of the system/path to remove shares for
   * @param systemId - Tapis system
   * @param path - path on system relative to system rootDir
   * @param recurse - if true include all sub-paths as well
   * @throws WebApplicationException - on error
   */
  public void removeAllSharesForPathWithoutAuth (
          String tenant, String systemId, String path, boolean recurse)
          throws WebApplicationException
  {
    String opName = "removeAllSharesForPath";

    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // Create request objects needed for SK calls.
    String pathToSearch = pathStr;
    // If recursive add "%" to the end of the path to search
    if (recurse) {
      pathToSearch = String.format("%s%s", pathStr, SK_WILDCARD);
    }

    var skGetParms = new SKShareGetSharesParms();
    skGetParms.setResourceType(RESOURCE_TYPE);
    skGetParms.setTenant(tenant);
    skGetParms.setResourceId1(systemId);
    skGetParms.setResourceId2(pathToSearch);

    // We will be calling SK which can throw TapisClientException. Handle it by converting to WebAppException
    try
    {
      // Get all shares for the path and it's sub-paths
      SkShareList shareList = getSKClient().getShares(skGetParms);
      // Remove share for every entry in the list
      if (shareList != null && shareList.getShares() != null)
      {
        for (SkShare skShare : shareList.getShares())
        {
          if (skShare.getId() != null) {
            getSKClient().deleteShareById(skShare.getId(), tenant);
          }
        }
      }
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsg("FILES_REMOVE_SHARE_ERR", OP_UNSHARE, tenant,
              systemId, path, pathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
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
    // Is the requester the owner of the system?
    boolean isOwner = rUser.getOboUserId().equals(sys.getOwner());

    // Get path relative to system rootDir and protect against ../.. and make sure starts with /
    String pathStr = PathUtils.getSKRelativePath(path).toString();

    // For convenience/clarity
    String oboUser = rUser.getOboUserId();
    String oboTenant = rUser.getOboTenantId();

    // User must be system owner
    if (!isOwner)
    {
      String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", oboTenant, oboUser, systemId, pathStr, opName);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }

    // Create request object needed for SK calls.
    ReqShareResource reqShareResource = null;
    SKShareDeleteShareParms deleteShareParms = null;
    switch (opName)
    {
      case OP_SHARE ->
      {
        reqShareResource = new ReqShareResource();
        reqShareResource.setResourceType(RESOURCE_TYPE);
        reqShareResource.setTenant(sys.getTenant());
        reqShareResource.setGrantor(oboUser);
        reqShareResource.setResourceId1(systemId);
        reqShareResource.setResourceId2(pathStr);
        reqShareResource.setPrivilege(FileInfo.Permission.READ.name());
      }
      case OP_UNSHARE ->
      {
        deleteShareParms = new SKShareDeleteShareParms();
        deleteShareParms.setResourceType(RESOURCE_TYPE);
        deleteShareParms.setTenant(sys.getTenant());
        deleteShareParms.setGrantor(oboUser);
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
      skShares = getSKClient().getShares(skParms);
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
   * @return path that is shared or null if no such path found.
   */
  private String checkForGranteeInParentPaths(SKShareGetSharesParms skParms, String pathStr)
          throws TapisClientException
  {
    // If path is empty or "/" then we are done.
    // We should never be given an empty string and if it is "/" then there are no parents and the calling routine
    //   will have already processed it.
    if (StringUtils.isBlank(pathStr) || "/".equals(pathStr)) return null;

    Path path = Paths.get(pathStr);
    SkShareList skShares;
    // walk parent paths up to root
    Path parentPath = path.getParent();
    while (parentPath != null)
    {
      // Get shares for the path
      String parentPathStr = parentPath.toString();
      skParms.setResourceId2(parentPathStr);
      skShares = getSKClient().getShares(skParms);
      // ====================================================
      // If share found then return true
      // ====================================================
      if (skShares != null && skShares.getShares() != null && !skShares.getShares().isEmpty()) return parentPathStr;
      // Get the next parent path to check
      parentPath = parentPath.getParent();
    }
    // No shares found
    return null;
  }


  /**
   * Get Security Kernel client with obo tenant and user set to the service tenant and user.
   * I.e. this is a client where the service calls SK as itself.
   * @return SK client
   * @throws TapisClientException - for Tapis related exceptions
   */
  private SKClient getSKClient() throws TapisClientException
  {
    // Init if necessary
    if (StringUtils.isBlank(svcTenantName))
    {
      // Site admin tenant may have been initialized statically, use it if available
      if (!StringUtils.isBlank(siteAdminTenantId))
      {
        svcTenantName = siteAdminTenantId;
      }
      else
      {
        // Not initialized statically, look it up
        String siteId = RuntimeSettings.get().getSiteId();
        svcTenantName = TenantManager.getInstance().getSiteAdminTenantId(siteId);
      }
    }
    return getSKClient(SERVICE_NAME, svcTenantName);
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
      log.error(msg, e);
      throw new TapisClientException(msg, e);
    }
  }
}
