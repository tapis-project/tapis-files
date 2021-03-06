package edu.utexas.tacc.tapis.files.api;

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
import edu.utexas.tacc.tapis.shared.security.ServiceClients;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.files.lib.caches.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.shared.ssh.apache.SSHConnection;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.ApplicationPath;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.TimeUnit;


// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse).
@OpenAPIDefinition(
        info = @Info(
                title = "Tapis Files API",
                version = "0.0",
                description = "My API",
                license = @License(name = "Apache 2.0", url = "http://foo.bar"),
                contact = @Contact(url = "http://tacc.utexas.edu", name = "CicSupport", email = "cicsupport@tacc.utexas.edu")
        ),
        tags = {
                @Tag(name = "file operations"),
                @Tag(name = "share"),
                @Tag(name = "permissions"),
                @Tag(name = "transfers")
        },
        security = {
                @SecurityRequirement(name = "Bearer"),
        },
        servers = {
                @Server(
                        description = "localhost",
                        url = "http://localhost:8080/"
                ),
                @Server(
                        description = "development",
                        url = "https://dev.develop.tapis.io"
                )
        }
)
@ApplicationPath("v3/files")
public class FilesApplication extends BaseResourceConfig {
    /**
     * BaseResourceConfig has all the extra jersey filters and our
     * custom ones for JWT validation and AuthZ
     */
    private static final Logger log = LoggerFactory.getLogger(FilesApplication.class);
    private IRuntimeConfig runtimeConfig;

    public FilesApplication() {
        super();

        runtimeConfig = RuntimeSettings.get();
        TenantManager tenantManager = TenantManager.getInstance(runtimeConfig.getTenantsServiceURL());
        tenantManager.getTenants();
        JWTValidateRequestFilter.setSiteId(runtimeConfig.getSiteId());
        JWTValidateRequestFilter.setService("files");
        SSHConnection.setLocalNodeName(runtimeConfig.getHostName());
        //JWT validation
        register(JWTValidateRequestFilter.class);

        //Our APIs
        register(ContentApiResource.class);
        register(TransfersApiResource.class);
        register(PermissionsApiResource.class);
        register(ShareApiResource.class);
        register(HealthApiResource.class);
        register(OperationsApiResource.class);
        register(UtilsLinuxApiResource.class);

        //OpenAPI jazz
        register(OpenApiResource.class);

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
		setApplicationName("files");
    }

    public static void main(String[] args) throws Exception {
        final URI BASE_URI = URI.create("http://0.0.0.0:8080/");
        FilesApplication config = new FilesApplication();

        final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
        Collection<NetworkListener> listeners = server.getListeners();
        for(NetworkListener listener : listeners) {
            log.info(listener.toString());
            final TCPNIOTransport transport = listener.getTransport();
            transport.setKeepAlive(true);
            transport.setWriteTimeout(0, TimeUnit.MINUTES);

            final ThreadPoolConfig tpc = transport.getWorkerThreadPoolConfig();
            tpc.setQueueLimit(-1).setCorePoolSize(100).setMaxPoolSize(100);
        }

        server.start();

    }
}
