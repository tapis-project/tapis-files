package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;

import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class Utils
{
  // Private constructor to make it non-instantiable
  private Utils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(Utils.class);

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
   * Standard perms check and throw given obo tenant+user, system, path, perm required.
   * @param svc - Perms service
   * @param oboTenant - obo tenant
   * @param oboUser - obo user
   * @param systemId - system
   * @param pathToCheck - path to check
   * @param perm - perm to check for
   * @throws NotAuthorizedException - perm check failed
   * @throws ServiceException - other exceptions
   */
  public static void checkPermitted(FilePermsService svc, String oboTenant, String oboUser, String systemId,
                                    Path pathToCheck, Permission perm)
          throws ForbiddenException, ServiceException
  {
    if (!svc.isPermitted(oboTenant, oboUser, systemId, pathToCheck.toString(), perm))
    {
      String msg = Utils.getMsg("FILES_NOT_AUTHORIZED", oboTenant, oboUser, systemId, pathToCheck, perm);
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
      throw new BadRequestException(getMsgAuth("FILES_SYS_NOTENABLED", authenticatedUser, sys.getId()));
  }
}
