package edu.utexas.tacc.tapis.files.api.utils;

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class ApiUtils
{
  // Private constructor to make it non-instantiable
  private ApiUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger log = LoggerFactory.getLogger(ApiUtils.class);

  // Location of message bundle files
  private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.files.api.FilesApiMessages";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsgAuth(String key, ResourceRequestUser rUser, Object... parms)
  {
    // Construct new array of parms. This appears to be most straightforward approach to modify and pass on varargs.
    var newParms = new Object[4 + parms.length];
    newParms[0] = rUser.getJwtTenantId();
    newParms[1] = rUser.getJwtUserId();
    newParms[2] = rUser.getOboTenantId();
    newParms[3] = rUser.getOboUserId();
    System.arraycopy(parms, 0, newParms, 4, parms.length);
    return getMsg(key, newParms);
  }

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale - Locale to use when building message. If null use default locale
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsg(String key, Locale locale, Object... parms)
  {
    String msgValue = null;

    if (locale == null) locale = Locale.getDefault();

    ResourceBundle bundle = null;
    try { bundle = ResourceBundle.getBundle(MESSAGE_BUNDLE, locale); }
    catch (Exception e)
    {
      log.error("Unable to find resource message bundle: " + MESSAGE_BUNDLE, e);
    }
    if (bundle != null) try { msgValue = bundle.getString(key); }
    catch (Exception e)
    {
      log.error("Unable to find key: " + key + " in resource message bundle: " + MESSAGE_BUNDLE, e);
    }

    if (msgValue != null)
    {
      // No problems. If needed fill in any placeholders in the message.
      if (parms != null && parms.length > 0) msgValue = MessageFormat.format(msgValue, parms);
    }
    else
    {
      // There was a problem. Build a message with as much info as we can give.
      StringBuilder sb = new StringBuilder("Key: ").append(key).append(" not found in bundle: ").append(MESSAGE_BUNDLE);
      if (parms != null && parms.length > 0)
      {
        sb.append("Parameters:[");
        for (Object parm : parms) {sb.append(parm.toString()).append(",");}
        sb.append("]");
      }
      msgValue = sb.toString();
    }
    return msgValue;
  }

  /**
   * Return single json element as a string
   * @param jelem Json element
   * @param defaultVal string value to use as a default if element is null
   * @return json element as string
   */
  public static String getValS(JsonElement jelem, String defaultVal)
  {
    if (jelem == null) return defaultVal;
    else return jelem.getAsString();
  }

  /**
   * ThreadContext.validate checks for tenantId, user, accountType, etc.
   * If all OK return null, else return error response.
   *
   * @param threadContext - thread context to check
   * @param prettyPrint - flag for pretty print of response
   * @return null if OK, else error response
   */
  public static Response checkContext(TapisThreadContext threadContext, boolean prettyPrint)
  {
    if (threadContext.validate()) return null;
    String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
    log.error(msg);
    return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
  }

  /**
   * Trace the incoming request, include info about requesting user, op name and request URL
   * @param rUser resource user
   * @param opName name of operation
   */
  public static void logRequest(ResourceRequestUser rUser, String className, String opName, String reqUrl, String... strParms)
  {
    // Build list of args passed in
    String argListStr = "";
    if (strParms != null && strParms.length > 0) argListStr = String.join(",", strParms);
    String msg = ApiUtils.getMsgAuth("FAPI_TRACE_REQUEST", rUser, className, opName, reqUrl, argListStr);
    log.trace(msg);
  }
}
