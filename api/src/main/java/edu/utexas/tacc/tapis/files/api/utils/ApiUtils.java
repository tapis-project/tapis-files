package edu.utexas.tacc.tapis.files.api.utils;

import com.google.gson.JsonElement;
//import edu.utexas.tacc.tapis.apps.model.App;
//import edu.utexas.tacc.tapis.apps.model.KeyValuePair;
//import edu.utexas.tacc.tapis.apps.model.NotificationMechanism;
//import edu.utexas.tacc.tapis.apps.model.NotificationSubscription;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.sharedapi.security.ResourceRequestUser;
//import edu.utexas.tacc.tapis.apps.service.AppsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

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

//  /**
//   * Check that app exists
//   * @param rUser - principal user containing tenant and user info
//   * @param appId - name of the app to check
//   * @param prettyPrint - print flag used to construct response
//   * @param opName - operation name, for constructing response msg
//   * @return - null if all checks OK else Response containing info
//   */
//  public static Response checkAppExists(AppsService appsService, ResourceRequestUser rUser,
//                                           String appId, boolean prettyPrint, String opName)
//  {
//    String msg;
//    boolean appExists;
//    try { appExists = appsService.checkForApp(rUser, appId); }
//    catch (Exception e)
//    {
//      msg = ApiUtils.getMsgAuth("APPAPI_CHECK_ERROR", rUser, appId, opName, e.getMessage());
//      log.error(msg, e);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
//    }
//    if (!appExists)
//    {
//      msg = ApiUtils.getMsgAuth("APPAPI_NOAPP", rUser, appId, opName);
//      log.error(msg);
//      return Response.status(Response.Status.NOT_FOUND).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
//    }
//    return null;
//  }
//
//  /**
//   * Build a list of lib model AppArg objects given the request objects
//   */
////  public static List<AppArg> buildLibAppArgs(List<ArgSpec> argSpecs)
////  {
////    if (argSpecs == null) return null;
////    var retList = new ArrayList<AppArg>();
////    if (argSpecs.isEmpty()) return retList;
////    for (ArgSpec argSpec : argSpecs)
////    {
////      AppArg appArg = new AppArg(argSpec.arg, argSpec.name, argSpec.description, argSpec.mode, argSpec.meta);
////      retList.add(appArg);
////    }
////    return retList;
////  }
//
////  /**
////   * Build a lib model ParameterSet object given the ParameterSet request object
////   */
////  public static edu.utexas.tacc.tapis.apps.model.ParameterSet buildLibParameterSet(ParameterSet parmSet)
////  {
////    if (parmSet == null) return null;
////    var retParmSet = new edu.utexas.tacc.tapis.apps.model.ParameterSet();
////    retParmSet.setAppArgs(buildLibAppArgs(parmSet.appArgs));
////    retParmSet.setAppArgs(buildLibAppArgs(parmSet.containerArgs));
////    retParmSet.setAppArgs(buildLibAppArgs(parmSet.schedulerOptions));
////    retParmSet.setEnvVariables(parmSet.envVariables);
////    // Handle ArchiveFilter
////    edu.utexas.tacc.tapis.apps.model.ArchiveFilter archiveFilter = null;
////    if (parmSet.archiveFilter != null)
////    {
////      archiveFilter = new ArchiveFilter();
////      // TODO includes, excludes
////      ??
////      archiveFilter.setIncludeLaunchFiles(parmSet.archiveFilter.includeLaunchFiles);
////    }
////    retParmSet.setArchiveFilter(archiveFilter);
////
////    return retParmSet;
////  }
////
////          ApiUtils.buildLibAppArgs(parmSet.appArgs), ApiUtils.buildLibAppArgs(parmSet.containerArgs),
////          ApiUtils.buildLibAppArgs(parmSet.schedulerOptions), envVariables, parmSet.archiveFilter.includes,
////          parmSet.archiveFilter.excludes, parmSet.archiveFilter.includeLaunchFiles,
//  /**
//   * Build a list of lib model FileInput objects given the request objects
//   */
//// TODO/TBD: do we really need this? Seems like with recent updates can now always use model class
////  public static List<edu.utexas.tacc.tapis.apps.model.FileInput> buildLibFileInputs(List<FileInput> fileInputs)
////  {
////    if (fileInputs == null) return null;
////    var retList = new ArrayList<edu.utexas.tacc.tapis.apps.model.FileInput>();
////    if (fileInputs.isEmpty()) return retList;
////    for (FileInput fid : fileInputs)
////    {
////      String[] kvPairs = ApiUtils.getKeyValuesAsArray(fid.meta);
////      edu.utexas.tacc.tapis.apps.model.FileInput fileInput =
////              new edu.utexas.tacc.tapis.apps.model.FileInput(fid.sourceUrl, fid.targetPath, fid.inPlace,
////                                          fid.name, fid.description, fid.mode, kvPairs);
////      retList.add(fileInput);
////    }
////    return retList;
////  }
//
////  /**
////   * Build a list of lib model subscription objects given the request api model objects
////   */
////  public static List<NotificationSubscription> buildLibNotifSubscriptions(List<NotificationSubscription> apiSubscriptions)
////  {
////    if (apiSubscriptions == null) return null;
////    var retList = new ArrayList<NotificationSubscription>();
////    if (apiSubscriptions.isEmpty()) return retList;
////    for (NotificationSubscription apiSubscription : apiSubscriptions)
////    {
////      NotificationSubscription libSubscription = new NotificationSubscription(apiSubscription.filter);
////      var apiMechanisms = apiSubscription.notificationMechanisms;
////      if (apiMechanisms == null || apiMechanisms.isEmpty()) apiMechanisms = new ArrayList<>();
////      var libMechanisms = new ArrayList<NotificationMechanism>();
////      for (NotificationMechanism apiMech : apiMechanisms)
////      {
////        var libMech = new NotificationMechanism(apiMech.mechanism, apiMech.webhookURL, apiMech.emailAddress);
////        libMechanisms.add(libMech);
////      }
////      libSubscription.setNotificationMechanisms(libMechanisms);
////      retList.add(libSubscription);
////    }
////    return retList;
////  }
//
//  // Build a list of api model file inputs based on the lib model objects
//// TODO/TBD: do we really need this? Seems like with recent updates can now always use model class
////  public static List<FileInput> buildApiFileInputs(List<edu.utexas.tacc.tapis.apps.model.FileInput> libFileInputs)
////  {
////    var retList = new ArrayList<FileInput>();
////    if (libFileInputs == null || libFileInputs.isEmpty()) return retList;
////    for (edu.utexas.tacc.tapis.apps.model.FileInput libFileInput : libFileInputs)
////    {
////      FileInput fid = new FileInput();
////      fid.name = libFileInput.getName();
////      fid.description = libFileInput.getDescription();
////      fid.mode = libFileInput.getInputMode();
////      fid.inPlace = libFileInput.isInPlace();
////      fid.sourceUrl = libFileInput.getSourceUrl();
////      fid.targetPath = libFileInput.getTargetPath();
////      fid.meta = ApiUtils.getKeyValuesAsList(libFileInput.getMeta());
////      retList.add(fid);
////    }
////    return retList;
////  }
//
////  // Build a list of api model subscriptions based on the lib model objects
////  public static List<NotificationMechanism> buildApiNotifMechanisms(List<NotificationMechanism> libMechanisms)
////  {
////    var retList = new ArrayList<NotificationMechanism>();
////    if (libMechanisms == null || libMechanisms.isEmpty()) return retList;
////    for (NotificationMechanism libMechanism : libMechanisms)
////    {
////      NotificationMechanism apiMechanism = new NotificationMechanism();
////      apiMechanism.mechanism = libMechanism.getMechanism();
////      apiMechanism.webhookURL = libMechanism.getWebhookUrl();
////      apiMechanism.emailAddress = libMechanism.getEmailAddress();
////      retList.add(apiMechanism);
////    }
////    return retList;
////  }
//
////  // Build a list of api model notif mechanisms based on the lib model objects
////  public static List<NotificationSubscription> buildApiNotifSubscriptions(List<NotificationSubscription> libSubscriptions)
////  {
////    var retList = new ArrayList<NotificationSubscription>();
////    if (libSubscriptions == null || libSubscriptions.isEmpty()) return retList;
////    for (NotificationSubscription libSubscription : libSubscriptions)
////    {
////      NotificationSubscription apiSubscription = new NotificationSubscription();
////      apiSubscription.filter = libSubscription.getFilter();
////      apiSubscription.notificationMechanisms = buildApiNotifMechanisms(libSubscription.getNotificationMechanisms());
////      retList.add(apiSubscription);
////    }
////    return retList;
////  }
//
//  /**
//   * Return String[] array of key=value given list of KeyValuePair
//   */
//  public static String[] getKeyValuesAsArray(List<KeyValuePair> kvList)
//  {
//    if (kvList == null) return null;
//    if (kvList.size() == 0) return App.EMPTY_STR_ARRAY;
//    return kvList.stream().map(KeyValuePair::toString).toArray(String[]::new);
//  }
//
//  /**
//   * Return list of KeyValuePair given String[] array of key=value
//   */
//  public static List<KeyValuePair> getKeyValuesAsList(String[] kvArray)
//  {
//    if (kvArray == null || kvArray.length == 0) return Collections.emptyList();
//    List<KeyValuePair> kvList = Arrays.stream(kvArray).map(KeyValuePair::fromString).collect(Collectors.toList());
//    return kvList;
//  }

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
