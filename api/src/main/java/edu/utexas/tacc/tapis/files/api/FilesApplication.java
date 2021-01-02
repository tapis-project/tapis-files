package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.lib.caches.FilePermsCache;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.services.FilePermsService;
import edu.utexas.tacc.tapis.files.lib.utils.ServiceJWTCacheFactory;
import edu.utexas.tacc.tapis.files.lib.utils.TenantCacheFactory;
import edu.utexas.tacc.tapis.files.api.resources.*;
import edu.utexas.tacc.tapis.shared.ssh.SSHConnectionCache;
import edu.utexas.tacc.tapis.files.lib.clients.RemoteDataClientFactory;
import edu.utexas.tacc.tapis.files.lib.config.IRuntimeConfig;
import edu.utexas.tacc.tapis.files.lib.config.RuntimeSettings;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;
import edu.utexas.tacc.tapis.shared.security.ServiceJWT;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.tenants.client.TenantsClient;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.ApplicationPath;
import java.net.URI;
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

        JWTValidateRequestFilter.setSiteId(runtimeConfig.getSiteId());
        JWTValidateRequestFilter.setService("files");
        //JWT validation
        register(JWTValidateRequestFilter.class);

        //Our APIs
        register(ContentApiResource.class);
        register(TransfersApiResource.class);
        register(PermissionsApiResource.class);
        register(ShareApiResource.class);
        register(HealthApiResource.class);
        register(OperationsApiResource.class);

        //OpenAPI jazz
        register(OpenApiResource.class);

        //For dependency injection into the Resource classes for testing.
        register(new AbstractBinder() {
            @Override
            protected void configure() {
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class);
                bindAsContract(SKClient.class);
                bindAsContract(SystemsClient.class);
                bindAsContract(TokensClient.class);
                bindAsContract(TenantsClient.class);
                bindAsContract(SystemsCache.class);
                bindAsContract(FilePermsService.class);
                bindAsContract(FilePermsCache.class);
                bind(new SSHConnectionCache(2, TimeUnit.MINUTES)).to(SSHConnectionCache.class);
                bindAsContract(RemoteDataClientFactory.class).in(Singleton.class);
                bindFactory(ServiceJWTCacheFactory.class).to(ServiceJWT.class).in(Singleton.class);
                bindFactory(TenantCacheFactory.class).to(TenantManager.class).in(Singleton.class);
            }
        });
		setApplicationName("files");
    }

    public static void main(String[] args) throws Exception {
        final URI BASE_URI = URI.create("http://0.0.0.0:8080/");
        FilesApplication config = new FilesApplication();

        final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
        final TCPNIOTransport transport = server.getListener("grizzly").getTransport();
        transport.setWorkerThreadPoolConfig(ThreadPoolConfig.defaultConfig().setCorePoolSize(16).setMaxPoolSize(32));
        server.start();
    }
}
