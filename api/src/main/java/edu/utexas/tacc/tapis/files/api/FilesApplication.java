package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.api.providers.FilesExceptionMapper;
import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.factories.ServiceContextFactory;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.services.FileUtilsService;
import edu.utexas.tacc.tapis.files.lib.services.IFileOpsService;
import edu.utexas.tacc.tapis.files.lib.providers.ServiceClientsFactory;
import edu.utexas.tacc.tapis.files.api.resources.*;
import edu.utexas.tacc.tapis.files.lib.services.IFileUtilsService;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.sharedapi.providers.ObjectMapperContextResolver;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.sharedapi.providers.ValidationExceptionMapper;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.inject.Singleton;
import javax.ws.rs.ApplicationPath;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.TimeUnit;


/*
 * Main startup class for the web application. Uses Jersey and Grizzly frameworks.
 *   Performs setup for HK2 dependency injection.
 *   Registers packages and features for Jersey.
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
 * For all logging use println or similar so we do not have a dependency on a logging subsystem.
 */
@ApplicationPath("/")
public class FilesApplication extends ResourceConfig
{
  // We must be running on a specific site and this will never change
  private static String siteId;
  public static String getSiteId() {return siteId;}
  private static String siteAdminTenantId;
  public static String getSiteAdminTenantId() {return siteAdminTenantId;}

  private IRuntimeConfig runtimeConfig;

  public FilesApplication()
  {
    super();
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
    // can't be implemented in a generic mapper
    register(FilesExceptionMapper.class);
    register(ValidationExceptionMapper.class);

    // AuthZ filters
    register(FilePermissionsAuthz.class);

    //JWT validation
    register(JWTValidateRequestFilter.class);

    //Our APIs
    register(ContentApiResource.class);
    register(TransfersApiResource.class);
    register(PermissionsApiResource.class);
    register(ShareApiResource.class);
    register(FilesResource.class);
    register(OperationsApiResource.class);
    register(UtilsLinuxApiResource.class);

    //OpenAPI jazz
//    register(OpenApiResource.class);

    // We specify what packages JAX-RS should recursively scan to find annotations. By setting the value to the
    // top-level directory in all projects, we can use JAX-RS annotations in any tapis class. In particular, the filter
    // classes in tapis-shared-api will be discovered whenever that project is included as a maven dependency.
    packages("edu.utexas.tacc.tapis");

    // Set the application name. Note that this has no impact on base URL
    setApplicationName(TapisConstants.SERVICE_NAME_FILES);

    // Perform remaining init steps in try block so we can print a fatal error message if something goes wrong.
    try {
      runtimeConfig = RuntimeSettings.get();
      siteId = runtimeConfig.getSiteId();

      String url = runtimeConfig.getTenantsServiceURL();
      TenantManager tenantManager = TenantManager.getInstance(url);
      tenantManager.getTenants();
      // Set admin tenant also, needed when building a client for calling other services (such as SK) as ourselves.
      siteAdminTenantId = tenantManager.getSiteAdminTenantId(siteId);

      JWTValidateRequestFilter.setSiteId(siteId);
      JWTValidateRequestFilter.setService(TapisConstants.SERVICE_NAME_FILES);
      SSHConnection.setLocalNodeName(runtimeConfig.getHostName());

      // Initialize bindings for HK2 dependency injection
      //For dependency injection into the Resource classes for testing.
      register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(new SSHConnectionCache(5, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class);
                bindAsContract(SystemsCache.class).in(Singleton.class);
                bindAsContract(FilePermsService.class).in(Singleton.class);
                bindAsContract(FilePermsCache.class).in(Singleton.class);
                bind(tenantManager).to(TenantManager.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bindFactory(ServiceClientsFactory.class).to(ServiceClients.class).in(Singleton.class);
                bindFactory(ServiceContextFactory.class).to(ServiceContext.class).in(Singleton.class);
                bind(FileOpsService.class).to(IFileOpsService.class).in(Singleton.class);
                bind(FileUtilsService.class).to(IFileUtilsService.class).in(Singleton.class);
          }
        });
    } catch (Exception e) {
      // This is a fatal error
      System.out.println("**** FAILURE TO INITIALIZE: Tapis Files Service ****");
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * Embedded Grizzly HTTP server
   */
  public static void main(String[] args) throws Exception {
    // If TAPIS_SERVICE_PORT set in env then use it.
    // Useful for starting service locally on a busy system where 8080 may not be available.
    String servicePort = System.getenv("TAPIS_SERVICE_PORT");
    if (StringUtils.isBlank(servicePort)) servicePort = "8080";

    // Set base protocol and port. If mainly running in k8s this may not need to be configurable.
    final URI baseUri = URI.create("http://0.0.0.0:" + servicePort + "/");

    // Initialize the application container
    FilesApplication config = new FilesApplication();

    // TODO/TBD - adapt from Apps to Files
    //  Initialize the service
    // In order to instantiate our service class using HK2 we need to create an application handler
    //   which allows us to get an injection manager which is used to get a locator.
    //   The locator is used get classes that have been registered using AbstractBinder.
    // NOTE: As of Jersey 2.26 dependency injection was abstracted out to make it easier to use DI frameworks
    //       other than HK2, although finding docs and examples on how to do so seems difficult.
//    ApplicationHandler handler = new ApplicationHandler(config);
//    InjectionManager im = handler.getInjectionManager();
//    ServiceLocator locator = im.getInstance(ServiceLocator.class);
//    AppsServiceImpl svcImpl = locator.getService(AppsServiceImpl.class);
//
//    // Call the main service init method
//    svcImpl.initService(siteId, siteAdminTenantId, RuntimeParameters.getInstance().getServicePassword());
//    // Create and start the server
//    final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config, false);
//    server.start();

    final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config, false);
    Collection<NetworkListener> listeners = server.getListeners();
    for (NetworkListener listener : listeners)
    {
      System.out.println("**** Listener: " + listener.toString() + " ****");
      final TCPNIOTransport transport = listener.getTransport();
      transport.setKeepAlive(true);
      transport.setWriteTimeout(0, TimeUnit.MINUTES);
      final ThreadPoolConfig tpc = transport.getWorkerThreadPoolConfig();
      tpc.setQueueLimit(-1).setCorePoolSize(100).setMaxPoolSize(100);
    }
    server.start();
  }
}
