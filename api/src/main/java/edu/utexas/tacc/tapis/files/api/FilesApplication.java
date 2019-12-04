package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.providers.ObjectMapperContextResolver;
import edu.utexas.tacc.tapis.files.api.resources.*;
import edu.utexas.tacc.tapis.files.lib.clients.FakeSystemsService;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.FileTransfersDAO;
import edu.utexas.tacc.tapis.sharedapi.providers.TapisExceptionMapper;
import edu.utexas.tacc.tapis.sharedapi.providers.ValidationExceptionMapper;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;
import java.net.URI;
import edu.utexas.tacc.tapis.sharedapi.jaxrs.filters.JWTValidateRequestFilter;



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
public class FilesApplication extends ResourceConfig {
    public FilesApplication() {

        // Need that for some reason for numtipart forms/ uploads
        register(MultiPartFeature.class);
        // Serialization
        register(JacksonFeature.class);
        // Custom Timestamp/Instant serialization
        register(ObjectMapperContextResolver.class);
        // Authentication Filter
        register(JWTValidateRequestFilter.class);
        // ExceptionMappers, need both because ValidationMapper is +
        register(TapisExceptionMapper.class);
        register(ValidationExceptionMapper.class);

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
                bindAsContract(FakeSystemsService.class);
                bindAsContract(FileTransfersDAO.class);
            }
        });

		setApplicationName("files");

    }

    public static void main(String[] args) throws Exception {
        final URI BASE_URI = URI.create("http://0.0.0.0:8080/files");
        ResourceConfig config = new FilesApplication();
        final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
        server.start();
    }
}
