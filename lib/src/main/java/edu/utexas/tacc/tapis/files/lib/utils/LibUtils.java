package edu.utexas.tacc.tapis.files.lib.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.shared.utils.PathUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_IMPERSONATE;
import static edu.utexas.tacc.tapis.files.lib.services.FileOpsService.SVCLIST_SHAREDCTX;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class LibUtils
{
  // Private constructor to make it non-instantiable
  private LibUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(LibUtils.class);

  // Location of message bundle files
  private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.files.lib.FilesMessages";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsgAuth(String key, AuthenticatedUser authUser, Object... parms)
  {
    // Construct new array of parms. This appears to be most straightforward approach to modify and pass on varargs.
    var newParms = new Object[4 + parms.length];
    newParms[0] = authUser.getTenantId();
    newParms[1] = authUser.getName();
    newParms[2] = authUser.getOboTenantId();
    newParms[3] = authUser.getOboUser();
    System.arraycopy(parms, 0, newParms, 4, parms.length);
    return getMsg(key, newParms);
  }

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsgAuthR(String key, ResourceRequestUser authUser, Object... parms)
  {
    // Construct new array of parms. This appears to be most straightforward approach to modify and pass on varargs.
    var newParms = new Object[4 + parms.length];
    newParms[0] = authUser.getJwtTenantId();
    newParms[1] = authUser.getJwtUserId();
    newParms[2] = authUser.getOboTenantId();
    newParms[3] = authUser.getOboUserId();
    System.arraycopy(parms, 0, newParms, 4, parms.length);
    return getMsg(key, newParms);
  }

  /**
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale Locale for message
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsg(String key, Locale locale, Object... parms)
  {
    String msgValue = null;

    if (locale == null) locale = Locale.getDefault();

    ResourceBundle bundle = null;
    try
    {
      bundle = ResourceBundle.getBundle(MESSAGE_BUNDLE, locale);
    } catch (Exception e)
    {
      log.error("Unable to find resource message bundle: " + MESSAGE_BUNDLE, e);
    }
    if (bundle != null) try
    {
      msgValue = bundle.getString(key);
    } catch (Exception e)
    {
      log.error("Unable to find key: " + key + " in resource message bundle: " + MESSAGE_BUNDLE, e);
    }

    if (msgValue != null)
    {
      // No problems. If needed fill in any placeholders in the message.
      if (parms != null && parms.length > 0) msgValue = MessageFormat.format(msgValue, parms);
    } else
    {
      // There was a problem. Build a message with as much info as we can give.
      StringBuilder sb = new StringBuilder("Key: ").append(key).append(" not found in bundle: ").append(MESSAGE_BUNDLE);
      if (parms != null && parms.length > 0)
      {
        sb.append("Parameters:[");
        for (Object parm : parms)
        {
          sb.append(parm.toString()).append(",");
        }
        sb.append("]");
      }
      msgValue = sb.toString();
    }
    return msgValue;
  }

  /**
   * Standard perms check and throw, given obo tenant+user, system, path, perm required.
   * Path can be the non-normalized path provided as part of the user request.
   * It will be normalized here. OK if already a normalized relative path.
   * @param svc - Perms service
   * @param oboTenant - obo tenant
   * @param oboUser - obo user
   * @param systemId - system
   * @param pathToCheck - path provided as part of request
   * @param perm - perm to check for
   * @throws ServiceException - other exceptions
   */
  public static void checkPermitted(FilePermsService svc, String oboTenant, String oboUser, String systemId,
                                    String pathToCheck, Permission perm)
          throws ForbiddenException, ServiceException
  {
    if (!svc.isPermitted(oboTenant, oboUser, systemId, PathUtils.getSKRelativePath(pathToCheck).toString(), perm))
    {
      String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", oboTenant, oboUser, systemId, pathToCheck, perm);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
  }

  /**
   * Perms check and throw, given obo tenant+user, system, path, perm required.
   * Path can be the non-normalized path provided as part of the user request.
   * It will be normalized here. OK if already a normalized relative path.
   * @param svc - Perms service
   * @param oboTenant - obo tenant
   * @param oboUser - obo user
   * @param systemId - system
   * @param pathToCheck - path provided as part of request
   * @throws ServiceException - other exceptions
   */
  public static void checkPermittedReadOrModify(FilePermsService svc, String oboTenant, String oboUser, String systemId,
                                                String pathToCheck)
          throws ForbiddenException, ServiceException
  {
    if (!svc.isPermitted(oboTenant, oboUser, systemId, PathUtils.getSKRelativePath(pathToCheck).toString(), Permission.READ)
        && !svc.isPermitted(oboTenant, oboUser, systemId, PathUtils.getSKRelativePath(pathToCheck).toString(), Permission.MODIFY))
    {
      String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", oboTenant, oboUser, systemId, pathToCheck, Permission.READ);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
  }

  /**
   * Standard check that system is available for use. If not available throw BadRequestException
   *   which results in a response status of BAD_REQUEST (400).
   */
  public static void checkEnabled(ResourceRequestUser rUser, TapisSystem sys) throws BadRequestException
  {
    if (sys.getEnabled() == null || !sys.getEnabled())
    {
      String msg = getMsgAuthR("FILES_SYS_NOTENABLED", rUser, sys.getId());
      log.warn(msg);
      throw new BadRequestException(msg);
    }
  }

  /**
   * Get a TapisSystem with credentials.
   * Include auth checks as appropriate for system and path
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sysId - system
   * @param relPathStr - normalized path to the file, dir or object. Relative to system rootDir
   * @param perm - required permission (READ or MODIFY)
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return fully resolved system with credentials
   * @throws ForbiddenException - oboUserId not authorized to perform operation
   */
  public static TapisSystem getResolvedSysWithAuthCheck(ResourceRequestUser rUser, FileShareService shareService,
                                                         SystemsCache sysCache, SystemsCacheNoAuth sysCacheNoAuth,
                                                         FilePermsService permsService, String opName,
                                                         String sysId, String relPathStr, Permission perm,
                                                         String impersonationId, String sharedCtxGrantor)
          throws ForbiddenException
  {
    String oboTenant = rUser.getOboTenantId();
    String oboUser = rUser.getOboUserId();
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    boolean isRead = Permission.READ.equals(perm);
    boolean isModify = Permission.MODIFY.equals(perm);

    // If sharedCtxGrantor set, confirm that it is allowed (can throw ForbiddenException)
    if (!StringUtils.isBlank(sharedCtxGrantor)) checkSharedCtxAllowed(rUser, opName, sysId, relPathStr, sharedCtxGrantor);
    // If impersonationId set, confirm that it is allowed (can throw ForbiddenException)
    if (!StringUtils.isBlank(impersonationId)) checkImpersonationAllowed(rUser, opName, sysId, relPathStr, impersonationId);

    // Get fully resolved system including credentials for oboOrImpersonationId
    // We will use a number of system attributes to determine if access is allowed.
    TapisSystem sys = getSystemIfEnabledNoAuth(rUser, sysCacheNoAuth, sysId, oboOrImpersonatedUser);

    // Check for ownership
    // ------------------------
    // If requester is owner of system or in shared context and share grantor is owner then allow.
    String sysOwner = sys.getOwner() == null ? "" : sys.getOwner();
    if (oboOrImpersonatedUser.equals(sysOwner) || sysOwner.equals(sharedCtxGrantor)) return sys;

    // Check for system sharing
    // ------------------------
    // Figure out if system is shared with requester publicly or directly (through the Systems service)
    boolean isSharedPublic = sys.getIsPublic() == null ? false : sys.getIsPublic();
    List<String> sharedWithUsers = sys.getSharedWithUsers();
    boolean isSharedDirect = (sharedWithUsers != null && sharedWithUsers.contains(oboOrImpersonatedUser));
    boolean isShared = (isSharedPublic || isSharedDirect);

    // If system is shared and perm request is READ then allow.
    if (isShared && isRead) return sys;

    // If system is shared and perm request is MODIFY and effectiveUserId is dynamic then allow.
    boolean systemIsDynamic = sys.getIsDynamicEffectiveUser() == null ? false : sys.getIsDynamicEffectiveUser();
    if (isShared && systemIsDynamic && isModify) return sys;

    // Check for file path sharing
    // ------------------------
    // NOTE: If path is shared then user has implicit access to system.
    boolean pathIsShared = isPathShared(rUser, shareService, sys, relPathStr, impersonationId, sharedCtxGrantor);
    // If file path shared and READ is requested then allow
    if (pathIsShared && isRead) return sys;

    // If file path shared, MODIFY requested and in shared context then allow
    // This allows a shared path to be used when running a job in a shared context.
    if (pathIsShared && isModify && !StringUtils.isBlank(sharedCtxGrantor)) return sys;

    // Check for fine-grained permission on path
    // If not owner and path not shared, user may still have fine-grained permission on path
    // Throws ForbiddenException if user does not have the requested perm for the path
    try
    {
      checkAuthForPath(rUser, permsService , sys, relPathStr, perm, impersonationId, sharedCtxGrantor);
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsg("FILES_OPSC_ERR", oboTenant, oboUser, opName, sysId, relPathStr, ex.getMessage());
      log.error(msg, ex);
      throw new WebApplicationException(msg, ex);
    }

    // So user has fine-grained perm for the path, but they still need at least READ for the system.
    // Check this by making an alternate call to a different cache that includes the check when fetching the system
    sys = getSystemIfEnabled(rUser, sysCache, sysId, impersonationId, sharedCtxGrantor);

    return sys;
  }

  /*
   * Convenience wrapper for callers that do not need to support sharing
   */
  public static TapisSystem getSystemIfEnabled(@NotNull ResourceRequestUser rUser, @NotNull SystemsCache systemsCache,
                                               @NotNull String systemId)
          throws NotFoundException
  {
    return getSystemIfEnabled(rUser, systemsCache, systemId, null, null);
  }

  /**
   * Check to see if a Tapis System exists and is enabled
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - System to check
   * @param systemsCache - Cache of systems
   * @throws NotFoundException System not found or not enabled
   */
  public static TapisSystem getSystemIfEnabled(@NotNull ResourceRequestUser rUser, @NotNull SystemsCache systemsCache,
                                               @NotNull String systemId, String impersonationId, String sharedCtxGrantor)
          throws NotFoundException
  {
    // Check for the system
    TapisSystem sys;
    try
    {
      sys = systemsCache.getSystem(rUser.getOboTenantId(), systemId, rUser.getOboUserId(), impersonationId, sharedCtxGrantor);
      if (sys == null)
      {
        String msg = LibUtils.getMsgAuthR("FILES_SYS_NOTFOUND", rUser, systemId);
        log.warn(msg);
        throw new NotFoundException(msg);
      }
      if (sys.getEnabled() == null || !sys.getEnabled())
      {
        String msg = LibUtils.getMsgAuthR("FILES_SYS_NOTENABLED", rUser, systemId);
        log.warn(msg);
        throw new NotFoundException(msg);
      }
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SYSOPS_ERR", rUser, "getSystemIfEnabled", systemId, ex.getMessage());
      log.warn(msg);
      throw new WebApplicationException(msg);
    }
    return sys;
  }

  /**
   * Construct message containing list of errors for
   */
  public static String getListOfTxfrErrors(ResourceRequestUser rUser, String txfrTaskTag, List<String> msgList)
  {
    var sb = new StringBuilder(LibUtils.getMsgAuthR("FILES_TXFR_ERRORLIST", rUser, txfrTaskTag));
    sb.append(System.lineSeparator());
    if (msgList == null || msgList.isEmpty()) return sb.toString();
    for (String msg : msgList) { sb.append("  ").append(msg).append(System.lineSeparator()); }
    return sb.toString();
  }

  // =============== DB Transaction Management ============================
  /**
   * Close any DB connection related artifacts that are not null
   * @throws SQLException - on sql error
   */
  public static void closeAndCommitDB(Connection conn, PreparedStatement pstmt, ResultSet rs) throws SQLException
  {
    if (rs != null) rs.close();
    if (pstmt != null) pstmt.close();
    if (conn != null) conn.commit();
  }

  /**
   * Roll back a DB transaction and throw an exception
   * This method always throws an exception, either IllegalStateException or TapisException
   */
  public static void rollbackDB(Connection conn, Exception e, String msgKey, Object... parms) throws TapisException
  {
    try
    {
      if (conn != null) conn.rollback();
    }
    catch (Exception e1)
    {
      log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
    }

    // If IllegalStateException or TapisException pass it back up
    if (e instanceof IllegalStateException) throw (IllegalStateException) e;
    if (e instanceof TapisException) throw (TapisException) e;

    // Log the exception.
    String msg = MsgUtils.getMsg(msgKey, parms);
    log.error(msg, e);
    throw new TapisException(msg, e);
  }

  /**
   * Close DB connection, typically called from finally block
   */
  public static void finalCloseDB(Connection conn)
  {
    // Always return the connection back to the connection pool.
    try
    {
      if (conn != null) conn.close();
    }
    catch (Exception e)
    {
      // If commit worked, we can swallow the exception.
      // If not, the commit exception will have been thrown.
      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
      log.error(msg, e);
    }
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */

  /**
   * Check to see if a Tapis System exists and is enabled.
   * Return fully resolve TapisSystem with credentials.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param cache - Cache of systems
   * @param systemId - System to check
   * @param tapisUser - oboUser or impersonationId to use as effective user for credentials.
   * @return fully resolved system with credentials.
   * @throws NotFoundException System not found or not enabled
   */
  private static TapisSystem getSystemIfEnabledNoAuth(@NotNull ResourceRequestUser rUser, @NotNull SystemsCacheNoAuth cache,
                                                      @NotNull String systemId, @NotNull String tapisUser)
          throws NotFoundException
  {
    // Check for the system
    TapisSystem sys;
    try
    {
      sys = cache.getSystem(rUser.getOboTenantId(), systemId, tapisUser);
      if (sys == null)
      {
        String msg = LibUtils.getMsgAuthR("FILES_SYS_NOTFOUND", rUser, systemId);
        log.warn(msg);
        throw new NotFoundException(msg);
      }
      if (sys.getEnabled() == null || !sys.getEnabled())
      {
        String msg = LibUtils.getMsgAuthR("FILES_SYS_NOTENABLED", rUser, systemId);
        log.warn(msg);
        throw new NotFoundException(msg);
      }
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SYSOPS_ERR", rUser, "getSystemIfEnabled", systemId, ex.getMessage());
      log.warn(msg);
      throw new WebApplicationException(msg);
    }
    return sys;
  }

  /**
   * Confirm that caller is allowed to set sharedCtxGrantor.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param opName - operation name
   * @param sysId - name of the system
   * @param relPathStr - path involved in operation, used for logging
   * @throws ForbiddenException - user not authorized to perform operation
   */
  private static void checkSharedCtxAllowed(ResourceRequestUser rUser, String opName, String sysId, String relPathStr,
                                            String sharedCtxGrantor)
          throws ForbiddenException
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_SHAREDCTX.contains(svcName))
    {
      String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_SHAREDCTX", rUser, opName, sysId, relPathStr, sharedCtxGrantor);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
    // An allowed service is using a sharedCtx, log it
    log.info(LibUtils.getMsgAuthR("FILES_AUTH_SHAREDCTX", rUser, opName, sysId, relPathStr, sharedCtxGrantor));
  }

  /**
   * Confirm that caller is allowed to set impersonationId.
   * Must be a service request from a service in the allowed list.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param opName - operation name
   * @param sysId - name of the system
   * @param pathStr - path involved in operation, used for logging
   * @throws ForbiddenException - user not authorized to perform operation
   */
  private static void checkImpersonationAllowed(ResourceRequestUser rUser, String opName, String sysId, String pathStr,
                                                String impersonationId)
          throws ForbiddenException
  {
    // If a service request the username will be the service name. E.g. files, jobs, streams, etc
    String svcName = rUser.getJwtUserId();
    if (!rUser.isServiceRequest() || !SVCLIST_IMPERSONATE.contains(svcName))
    {
      String msg = LibUtils.getMsgAuthR("FILES_UNAUTH_IMPERSONATE", rUser, opName, sysId, pathStr, impersonationId);
      log.warn(msg);
      throw new ForbiddenException(msg);
    }
    // An allowed service is using impersonation, log it
    log.info(LibUtils.getMsgAuthR("FILES_AUTH_IMPERSONATE", rUser, opName, sysId, pathStr, impersonationId));
  }

  /**
   * Determine if file path is shared with oboOrImpersonatedUser or share grantor
   * Relative path should have already been normalized
   * We do not check here that impersonation is allowed.
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param sys - System
   * @param relPathStr - path on system relative to system rootDir
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @return true if shared else false
   */
  private static boolean isPathShared(@NotNull ResourceRequestUser rUser, FileShareService shareService,
                                      @NotNull TapisSystem sys, @NotNull String relPathStr,
                                      String impersonationId, String sharedCtxGrantor)
          throws WebApplicationException
  {
    // Certain services are allowed to impersonate an OBO user for the purposes of authorization
    //   and effectiveUserId resolution.
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    try
    {
      if (shareService.isSharedWithUser(rUser, sys, relPathStr, oboOrImpersonatedUser)) return true;
      if (!StringUtils.isBlank(sharedCtxGrantor) && shareService.isSharedWithUser(rUser, sys, relPathStr, sharedCtxGrantor)) return true;
    }
    catch (TapisClientException e)
    {
      String msg = LibUtils.getMsgAuthR("FILES_SHARE_GET_ERR", rUser, sys.getId(), relPathStr, e.getMessage());
      log.error(msg, e);
      throw new WebApplicationException(msg, e);
    }
    return false;
  }

  /**
   * Confirm that caller, impersonationId or sharedCtxGrantor has READ and/or MODIFY permission on a path
   * If READ perm passed in then can be READ or MODIFY. If MODIFY passed in then it must be MODIFY.
   *
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param system - system containing path
   * @param relPathStr - normalized path to the file, dir or object. Relative to system rootDir
   * @param perm - required permission (READ or MODIFY)
   * @param impersonationId - use provided Tapis username instead of oboUser
   * @param sharedCtxGrantor - Share grantor for the case of a shared context.
   * @throws ForbiddenException - oboUserId not authorized to perform operation
   */
  private static void checkAuthForPath(ResourceRequestUser rUser, FilePermsService permsService,
                                       TapisSystem system, String relPathStr, Permission perm,
                                       String impersonationId, String sharedCtxGrantor)
          throws ForbiddenException, ServiceException
  {
    String sysId = system.getId();
    String sysOwner = system.getOwner();
    String oboTenant = rUser.getOboTenantId();
    boolean modifyRequired = Permission.MODIFY.equals(perm);
    String oboOrImpersonatedUser = StringUtils.isBlank(impersonationId) ? rUser.getOboUserId() : impersonationId;
    boolean sharedCtx = !StringUtils.isBlank(sharedCtxGrantor);

    // If obo user is owner or in shared context and share grantor is owner then allow.
    if (oboOrImpersonatedUser.equals(sysOwner) || (sharedCtx && sharedCtxGrantor.equals(sysOwner))) return;

    // Check for fine-grained permission READ/MODIFY or MODIFY for obo user
    if (modifyRequired)
    {
      if (permsService.isPermitted(oboTenant, oboOrImpersonatedUser, sysId, relPathStr, Permission.MODIFY)) return;
    }
    else
    {
      if (permsService.isPermitted(oboTenant, oboOrImpersonatedUser, sysId, relPathStr, Permission.READ) ||
              permsService.isPermitted(oboTenant, oboOrImpersonatedUser, sysId, relPathStr, Permission.MODIFY)) return;
    }

    // Check for fine-grained permission READ/MODIFY or MODIFY for share grantor
    if (sharedCtx && modifyRequired)
    {
      if (permsService.isPermitted(oboTenant, sharedCtxGrantor, sysId, relPathStr, Permission.MODIFY)) return;
    }
    else if (sharedCtx)
    {
      if (permsService.isPermitted(oboTenant, sharedCtxGrantor, sysId, relPathStr, Permission.READ) ||
              permsService.isPermitted(oboTenant, sharedCtxGrantor, sysId, relPathStr, Permission.MODIFY)) return;
    }

    // No fine-grained permissions allowed the operation. Throw ForbiddenException
    String msg = LibUtils.getMsg("FILES_NOT_AUTHORIZED", rUser.getOboTenantId(), oboOrImpersonatedUser, sysId,
            relPathStr, perm);
    log.warn(msg);
    throw new ForbiddenException(msg);
  }
}
