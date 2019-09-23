package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.filters.TapisJWTFilter;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;
import java.net.URI;

// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("files")
public class FilesApplication extends ResourceConfig
{
	public FilesApplication()
	{
		register(ListingsResource.class);

		register(JacksonFeature.class);
		register(TapisJWTFilter.class);

		OpenApiResource openApiResource = new OpenApiResource();
		register(openApiResource);
		setApplicationName("files");

	}

	public static void main(String[] args) throws Exception {
		final URI BASE_URI = URI.create("http://0.0.0.0:8080/files");
		ResourceConfig config = new FilesApplication();
		System.out.println(config.getResources());
		final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
		server.start();
	}
}
