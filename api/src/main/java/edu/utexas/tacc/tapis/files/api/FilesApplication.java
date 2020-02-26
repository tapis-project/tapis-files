package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.factories.FileOpsServiceFactory;
import edu.utexas.tacc.tapis.files.api.resources.*;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.files.lib.services.FileOpsService;
import edu.utexas.tacc.tapis.files.lib.services.TransfersService;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.sharedapi.security.TenantManager;
import edu.utexas.tacc.tapis.systems.client.SystemsClient;
import edu.utexas.tacc.tapis.tokens.client.TokensClient;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.logging.Log;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.net.URI;


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
                        url = "https://dev.develop.tapis.io/v3"
                )
        }
)
@ApplicationPath("/files")
public class FilesApplication extends BaseResourceConfig {
    /**
     * BaseResourceConfig has all the extra jersey filters and our
     * custom ones for JWT validation and AuthZ
     */


    public FilesApplication() {
        super();
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
                bindFactory(FileOpsServiceFactory.class).to(FileOpsService.class).in(RequestScoped.class);
                bindAsContract(FileTransfersDAO.class);
                bindAsContract(TransfersService.class);
                bindAsContract(SKClient.class);
                bindAsContract(SystemsClient.class);
                bindAsContract(TokensClient.class);
            }
        });

		setApplicationName("files");

        // Initialize tenant manager singleton. This can be used by all subsequent application code, including filters.
        try {
            // The base url of the tenants service is a required input parameter.
            // Retrieve the tenant list from the tenant service now to fail fast if we can't access the list.
            TenantManager.getInstance("https://dev.develop.tapis.io").getTenants();
        } catch (Exception e) {
            // This is a fatal error
            System.out.println("**** FAILURE TO INITIALIZE: tapis-systemsapi ****");
            e.printStackTrace();
            throw e;
        }

    }

    public static void main(String[] args) throws Exception {
        final URI BASE_URI = URI.create("http://0.0.0.0:8080/files");
        ResourceConfig config = new FilesApplication();
        final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
        server.start();
    }
}
