package edu.utexas.tacc.tapis.files.api;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCacheNoAuth;
import edu.utexas.tacc.tapis.files.lib.caches.TenantAdminCache;
import edu.utexas.tacc.tapis.files.lib.dao.postits.PostItsDAO;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileShareService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.api.resources.*;
import edu.utexas.tacc.tapis.files.lib.services.PostItsService;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPoolPolicy;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.ClearThreadLocalRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.ClearThreadLocalResponseFilter;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.QueryParametersRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.TestParameterRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.providers.ApiExceptionMapper;
import edu.utexas.tacc.tapis.sharedapi.providers.ObjectMapperContextResolver;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.providers.ValidationExceptionMapper;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.internal.inject.InjectionManager;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.ApplicationPath;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
 * Main startup class for the web application. Uses Jersey and Grizzly frameworks.
 *   Performs setup for HK2 dependency injection.
 *   Register packages and features for Jersey.
 *   Gets runtime parameters from the environment.
 *   Initializes the service:
 *     Init service context.
 *     DB creation or migration
 *   Starts the Grizzly server.
 *
 * The path here is appended to the context root and is configured to work when invoked in a standalone
 * container (command line) and in an IDE (such as eclipse).
 * ApplicationPath set to "/" since each resource class includes "/v3/files" in the
 *     path set at the class level. See OperationsApiResource.java, PermissionsApiResource.java, etc.
 *     This has been found to be a more robust scheme for keeping startup working for both
 *     running in an IDE and standalone.
 *
 * For all logging use println or similar, so we do not have a dependency on a logging subsystem.
 */
@ApplicationPath("/")
public class FilesApplication extends ResourceConfig
{
  private static Logger log = LoggerFactory.getLogger(FilesApplication.class);
  // SSHConnection cache settings
  public static final long SSHCACHE_TIMEOUT_MINUTES = 5;

  // reread the logging config file every 5 minutes
  private static final int REREAD_LOGFILE_INTERVAL_SECS = 300;

  // We must be running on a specific site and this will never change
  private static String siteId;
  public static String getSiteId() {return siteId;}
  private static String siteAdminTenantId;
  public static String getSiteAdminTenantId() {return siteAdminTenantId;}

  private static ScheduledExecutorService loggerExecutorService;

  public FilesApplication()
  {
    // Log our existence.
    // Output version information on startup
    System.out.println("**** Starting Files Service. Version: " + TapisUtils.getTapisFullVersion() + " ****");

    // TODO: Some of these class registrations are also in BaseResourceConfig but cannot delete
    //       BaseResourceConfig yet because it is used in 5 test classes. Review Systems and Apps code to see if
    //       BaseResourceConfig can be deleted or if these registrations should be removed and the Base used instead.

    // Needed for properly returning timestamps
    // Also allows for setting a breakpoint when response is being constructed.
    register(ObjectMapperContextResolver.class);

    // Need this for some reason for multipart forms/ uploads
    register(MultiPartFeature.class);

    // Serialization
    register(JacksonFeature.class);

    // ExceptionMappers, need both because ValidationMapper is a custom Jersey thing and
    // cannot be implemented in a generic mapper
    register(ApiExceptionMapper.class);
    register(ValidationExceptionMapper.class);

    // Register service class for calling init method during application startup
    register(FileOpsService.class);

    // AuthZ filters
    register(FilePermissionsAuthz.class);

    //JWT validation
    register(JWTValidateRequestFilter.class);

    //Our APIs
    register(ContentApiResource.class);
    register(TransfersApiResource.class);
    register(PermissionsApiResource.class);
    register(ShareResource.class);
    register(GeneralResource.class);
    register(OperationsApiResource.class);
    register(UtilsLinuxApiResource.class);
    register(PostItsResource.class);

    // we were previously calling - packages("edu.utexas.tacc.tapis");
    // this was inadvertently bringing in TapisExceptionMapper.  I removed that, and replaced it with
    // these register calls which are the classes (other than TapisExceptionMapper) that were being
    // included by packages()
    register(ClearThreadLocalRequestFilter.class);
    register(QueryParametersRequestFilter.class);
    register(ClearThreadLocalResponseFilter.class);
    register(edu.utexas.tacc.tapis.files.api.providers.ObjectMapperContextResolver.class);
    register(TestParameterRequestFilter.class);
    // end of classes added when removing packages()

    // Set the application name. Note that this has no impact on base URL
    setApplicationName(TapisConstants.SERVICE_NAME_FILES);

    // Perform remaining init steps in try block, so we can print a fatal error message if something goes wrong.
    try
    {
      // Get runtime parameters
      IRuntimeConfig runtimeConfig = RuntimeSettings.get();

      // Set site on which we are running. This is a required runtime parameter.
      siteId = runtimeConfig.getSiteId();

      // Init tenant manager, site admin tenant, JWT filter
      String url = runtimeConfig.getTenantsServiceURL();
      TenantManager tenantManager = TenantManager.getInstance(url);
      tenantManager.getTenants();
      // Set admin tenant also, needed when building a client for calling other services (such as SK) as ourselves.
      siteAdminTenantId = tenantManager.getSiteAdminTenantId(siteId);

      // Initialize security filter used when processing a request.
      JWTValidateRequestFilter.setSiteId(siteId);
      JWTValidateRequestFilter.setService(TapisConstants.SERVICE_NAME_FILES);
      SSHConnection.setLocalNodeName(runtimeConfig.getHostName());

      // Initialize bindings for HK2 dependency injection
      //For dependency injection into the Resource classes for testing.
      register(new AbstractBinder()
      {
        @Override
        protected void configure() {
          bind(tenantManager).to(TenantManager.class);
          bindAsContract(FileOpsService.class).in(Singleton.class);
          bindAsContract(FileUtilsService.class).in(Singleton.class);
          bindAsContract(FileTransfersDAO.class);
          bindAsContract(TransfersService.class);
          bindAsContract(SystemsCache.class).in(Singleton.class);
          bindAsContract(SystemsCacheNoAuth.class).in(Singleton.class);
          bindAsContract(FilePermsService.class).in(Singleton.class);
          bindAsContract(FilePermsCache.class).in(Singleton.class);
          bindAsContract(TenantAdminCache.class).in(Singleton.class);
          bindAsContract(FileShareService.class).in(Singleton.class);
          bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
          bindAsContract(PostItsService.class).in(Singleton.class);
          bindAsContract(PostItsDAO.class).in(Singleton.class);
          bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
          bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
        }
      });

      SshSessionPoolPolicy poolPolicy = SshSessionPoolPolicy.defaultPolicy()
              .setMaxConnectionDuration(Duration.ofHours(6))
              .setMaxConnectionIdleTime(Duration.ofMinutes(15))
              .setMaxConnectionsPerKey(1)
              .setMaxSessionsPerConnection(8)
              .setCleanupInterval(Duration.ofSeconds(15))
              .setTraceDuringCleanupFrequency(RuntimeSettings.get().getSshPoolTraceOnCleanupInterval())
              .setSessionCreationStrategy(SshSessionPoolPolicy.SessionCreationStrategy.MINIMIZE_CONNECTIONS);
      SshSessionPool.init(poolPolicy);

    }
    catch (Exception e)
    {
      // This is a fatal error
      System.out.println("**** FAILURE TO INITIALIZE: Tapis Files Service ****");
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Main method to init service and start embedded Grizzly HTTP server
   */
  public static void main(String[] args) throws Exception
  {
    // If TAPIS_SERVICE_PORT set in env then use it.
    // Useful for starting service locally on a busy system where 8080 may not be available.
    String servicePort = System.getenv("TAPIS_SERVICE_PORT");
    if (StringUtils.isBlank(servicePort)) servicePort = "8080";

    // Set base protocol and port. If mainly running in k8s this may not need to be configurable.
    final URI baseUri = URI.create("http://0.0.0.0:" + servicePort + "/");

    // Initialize the application container
    FilesApplication config = new FilesApplication();

    // Initialize the service
    // In order to instantiate our service class using HK2 we need to create an application handler
    //   which allows us to get an injection manager which is used to get a locator.
    //   The locator is used get classes that have been registered using AbstractBinder.
    // NOTE: As of Jersey 2.26 dependency injection was abstracted out to make it easier to use DI frameworks
    //       other than HK2, although finding docs and examples on how to do so seems difficult.
    ApplicationHandler handler = new ApplicationHandler(config);
    InjectionManager im = handler.getInjectionManager();
    ServiceLocator locator = im.getInstance(ServiceLocator.class);
    FileOpsService svcImpl = locator.getService(FileOpsService.class);

    // Call the main service init method
    System.out.println("Initializing service");
    svcImpl.initService(siteId, siteAdminTenantId, RuntimeSettings.get().getServicePassword());

    // Create and start the server
    System.out.println("Starting http server");
    final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config, false);
    Collection<NetworkListener> listeners = server.getListeners();
    for (NetworkListener listener : listeners)
    {
      System.out.println("**** Listener: " + listener.toString() + " ****");
      final TCPNIOTransport transport = listener.getTransport();
      transport.setKeepAlive(true);
      transport.setWriteTimeout(0, TimeUnit.MINUTES);
      final ThreadPoolConfig tpc = transport.getWorkerThreadPoolConfig();
      tpc.setQueueLimit(-1).setCorePoolSize(RuntimeSettings.get().getDbConnectionPoolCoreSize())
              .setMaxPoolSize(RuntimeSettings.get().getDbConnectionPoolSize());
    }
    server.start();
    PostItsService postItsService = locator.getService(PostItsService.class);

    Thread filesShutdownThread = new FilesShutdownThread(postItsService);
    Runtime.getRuntime().addShutdownHook(filesShutdownThread);
    postItsService.startPostItsReaper(RuntimeSettings.get().getPostItsReaperIntervalMinutes());
    loggerExecutorService = Executors.newSingleThreadScheduledExecutor();
    loggerExecutorService.scheduleAtFixedRate(() -> { rereadLoggerConfiguration(); },
            RuntimeSettings.get().getRereadLogConfigIntevalSeconds(),
            RuntimeSettings.get().getRereadLogConfigIntevalSeconds(), TimeUnit.SECONDS);
  }

  private static void rereadLoggerConfiguration() {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    ContextInitializer contextInitializer = new ContextInitializer(loggerContext);
    URL url = contextInitializer.findURLOfDefaultConfigurationFile(true);
    try {
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(loggerContext);
      loggerContext.reset();
      configurator.doConfigure(url);
    } catch (JoranException ex) {
      log.error("Unable to re-read logback.xml file");
    }
  }

  private static class FilesShutdownThread extends Thread {
    private final PostItsService postItsService;

    public FilesShutdownThread(PostItsService postItsService) {
      this.postItsService = postItsService;
    }
    @Override
    public void run() {
      postItsService.shutdown();
    }
  }
}
