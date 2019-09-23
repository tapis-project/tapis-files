package edu.utexas.tacc.tapis.files.api.filters;


import java.io.IOException;
import java.security.KeyStoreException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;

import edu.utexas.tacc.tapis.files.api.security.TapisSecurtiyContext;
import edu.utexas.tacc.tapis.files.api.security.AuthenticatedUser;

@Provider
@Priority(Priorities.AUTHENTICATION)
public class TapisJWTFilter implements ContainerRequestFilter {

  private static final Logger log = LoggerFactory.getLogger(TapisJWTFilter.class);

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    String authHeader;
    Jwt jwt;

    MultivaluedMap<String, String> headers = requestContext.getHeaders();
    authHeader = headers.getFirst("Authorization");
    if (authHeader == null) {
      requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
    }

    // Strip out the leading Bearer part of the Authorization header, leaving us with just the string JWT
    authHeader.replace("Bearer ", "");

    try {
      jwt = Jwts.parser().parse(authHeader);
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
}
