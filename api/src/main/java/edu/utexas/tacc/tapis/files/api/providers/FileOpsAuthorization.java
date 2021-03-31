package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.files.lib.models.FileInfo.Permission;

import javax.annotation.Priority;
import javax.ws.rs.NameBinding;
import javax.ws.rs.Priorities;
import java.lang.annotation.*;


/**
 * Annotation for doing authz for files. Place it on a method like such:
 * @GET
 * @FileOpsAuthorization(permsRequired = FilePermissionsEnum.READ)
 * public Response getSomething() {
 *  yada...yada..
 * }
 */
@Documented
@NameBinding
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Priority(Priorities.AUTHORIZATION)
public @interface FileOpsAuthorization {

    Permission permRequired() default Permission.READ;

}