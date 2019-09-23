package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.resources.PermissionsApiResource;
import edu.utexas.tacc.tapis.files.api.resources.ShareApiResource;
import edu.utexas.tacc.tapis.files.api.resources.SystemsApiResource;
import edu.utexas.tacc.tapis.files.api.resources.TransfersApiResource;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;
import java.net.URI;
import edu.utexas.tacc.tapis.files.api.filters.TapisJWTFilter;


// The path here is appended to the context root and
// is configured to work when invoked in a standalone 
// container (command line) and in an IDE (eclipse). 
@ApplicationPath("files")
public class FilesApplication extends ResourceConfig {
	public FilesApplication() {

	  register(MultiPartFeature.class);
		register(JacksonFeature.class);
		register(TapisJWTFilter.class);
		register(SystemsApiResource.class);
		register(TransfersApiResource.class);
    register(PermissionsApiResource.class);
    register(ShareApiResource.class);

		register(OpenApiResource.class);
		setApplicationName("files");

	}

	public static void main(String[] args) throws Exception {
		final URI BASE_URI = URI.create("http://0.0.0.0:9999");
		ResourceConfig config = new FilesApplication();
		System.out.println(config.getResources());
		final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, config, false);
		server.start();
	}
}
