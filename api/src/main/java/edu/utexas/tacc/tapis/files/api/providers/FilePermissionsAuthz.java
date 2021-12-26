package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.files.lib.caches.SystemsCache;
import edu.utexas.tacc.tapis.files.lib.exceptions.ServiceException;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.client.gen.model.TapisSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.Objects;

/**
 *  Annotation for The File Permissions endpoints. Currently, only the *owner* of the system
 *  is allowed to grant/revoke permissions. API users can check their own permissions
 *  via the GET endpoint, which is not protected by this annotation.
 */
@FilePermissionsAuthorization
public class FilePermissionsAuthz implements ContainerRequestFilter
{
  private final Logger log = LoggerFactory.getLogger(FilePermissionsAuthz.class);

  @Inject private SystemsCache systemsCache;

  @Context
  private ResourceInfo resourceInfo;

  @Override
  public void filter(ContainerRequestContext requestContext) throws ForbiddenException, IOException {

    AuthenticatedUser user = (AuthenticatedUser) requestContext.getSecurityContext().getUserPrincipal();
    String username = user.getOboUser();
    String tenantId = user.getOboTenantId();
    MultivaluedMap<String, String> params = requestContext.getUriInfo().getPathParameters();
    String systemId = params.getFirst("systemId");

    try
    {
      TapisSystem system = systemsCache.getSystem(tenantId, systemId, username);
      if (!Objects.equals(system.getOwner(), username)) {
        throw new ForbiddenException(LibUtils.getMsgAuth("FILES_PERM_NOT_AUTH", user, systemId));
      }
    }
    catch (ServiceException ex)
    {
      String msg = LibUtils.getMsgAuth("FILES_PERM_ERR", user, "authorization", systemId, ex.getMessage());
      log.error(msg, ex);
      throw new IOException(msg, ex);
    }
  }
}
