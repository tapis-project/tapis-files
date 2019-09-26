package edu.utexas.tacc.tapis.files.api.filters;


import java.io.IOException;
import java.lang.reflect.Method;

import javax.annotation.Priority;
import javax.annotation.security.PermitAll;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;

import edu.utexas.tacc.tapis.files.api.security.TapisSecurtiyContext;
import edu.utexas.tacc.tapis.files.api.security.AuthenticatedUser;

@Provider
@Priority(Priorities.AUTHENTICATION)
public class TapisAuthenticationFilter implements ContainerRequestFilter {

  private static final Logger log = LoggerFactory.getLogger(TapisAuthenticationFilter.class);
  private static final String AUTH_HEADER = "Authorization";

  @Context
  private ResourceInfo resourceInfo;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    Method method = resourceInfo.getResourceMethod();

    String authHeader;
    Jwt jwt;

    // @PermitAll on the method takes precedence over @RolesAllowed on the class
    if (method.isAnnotationPresent(PermitAll.class)) {
      // Do nothing
      return;
    }


    MultivaluedMap<String, String> headers = requestContext.getHeaders();
    authHeader = headers.getFirst(AUTH_HEADER);
    if (authHeader == null) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }

    // Strip out the leading Bearer part of the Authorization header, leaving us with just the string JWT
    authHeader = authHeader.replace("Bearer ", "");

    try {
      jwt = decodeJWT(authHeader);
      Claims claims = (Claims) jwt.getBody();
      String username = (String) claims.get("username");
      String tenantId = (String) claims.get("tenantId");
      String roles = (String) claims.get("roles");
      AuthenticatedUser user = new AuthenticatedUser(username, tenantId, roles, jwt);
      requestContext.setSecurityContext(new TapisSecurtiyContext(user));
    } catch (Exception e) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }
  }

  private Jwt decodeJWT(@NotNull String encodedJWT) {
    String tmp = encodedJWT.substring(0, encodedJWT.lastIndexOf(".")+1);
    Jwt j = Jwts.parser().parse(tmp);
    return j;
  }
}
