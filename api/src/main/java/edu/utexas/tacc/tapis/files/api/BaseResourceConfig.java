package edu.utexas.tacc.tapis.files.api;

import edu.utexas.tacc.tapis.files.api.providers.FileOpsAuthzSystemPath;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthorization;
import edu.utexas.tacc.tapis.files.api.providers.FilePermissionsAuthz;
import edu.utexas.tacc.tapis.files.api.providers.ObjectMapperContextResolver;
import edu.utexas.tacc.tapis.sharedapi.providers.TapisExceptionMapper;
import edu.utexas.tacc.tapis.sharedapi.providers.ValidationExceptionMapper;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

public class BaseResourceConfig extends ResourceConfig {

    public BaseResourceConfig() {
        super();
        // Need that for some reason for multipart forms/ uploads
        register(MultiPartFeature.class);
        // Serialization
        register(JacksonFeature.class);
        // Custom Timestamp/Instant serialization
        register(ObjectMapperContextResolver.class);
        // ExceptionMappers, need both because ValidationMapper is a custom Jersey thing and
        // can't be implemented in a generic mapper
        register(TapisExceptionMapper.class);
        register(ValidationExceptionMapper.class);

        // AuthZ filters
        register(FileOpsAuthzSystemPath.class);
        register(FilePermissionsAuthz.class);
    }
}
