package edu.utexas.tacc.tapis.files.lib.utils;

import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;

import edu.utexas.tacc.tapis.files.lib.caches.ISystemsCache;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;

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
   * @throws NotAuthorizedException - perm check failed
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
   * @throws NotAuthorizedException - perm check failed
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
   * Mapping happens in edu.utexas.tacc.tapis.sharedapi.providers.TapisExceptionMapper
   */
  public static void checkEnabled(AuthenticatedUser authenticatedUser, TapisSystem sys) throws BadRequestException
  {
    if (sys.getEnabled() == null || !sys.getEnabled())
    {
      String msg = getMsgAuth("FILES_SYS_NOTENABLED", authenticatedUser, sys.getId());
      log.warn(msg);
      throw new BadRequestException(msg);
    }
  }

  /**
   * Check to see if a Tapis System exists and is enabled
   * @param rUser - ResourceRequestUser containing tenant, user and request info
   * @param systemId - System to check
   * @param systemsCache - Cache that includes authorization
   * @throws NotFoundException System not found or not enabled
   */
  public static TapisSystem getSystemIfEnabled(@NotNull ResourceRequestUser rUser, @NotNull ISystemsCache systemsCache,
                                               @NotNull String systemId)
          throws NotFoundException
  {
    // Check for the system
    TapisSystem sys;
    try
    {
      sys = systemsCache.getSystem(rUser.getOboTenantId(), systemId, rUser.getOboUserId());
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
      String msg = LibUtils.getMsgAuthR("FILES_SYS_NOTFOUND", rUser, systemId);
      log.warn(msg);
      throw new NotFoundException(msg);
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
}
