package edu.utexas.tacc.tapis.files.api.providers;

import edu.utexas.tacc.tapis.sharedapi.responses.TapisResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import static javax.ws.rs.core.Response.Status.*;

/*
 * Class used to map various exception types to a TapisResponse.
 * Support for mapping:
 *    NotFound, NotAuthorized, Forbidden, BadRequest and WebApplication.
 * Other exception types result in an UNCAUGHT_EXCEPTION error and generation of an INTERNAL_SERVER_ERROR response.
 */
public class FilesExceptionMapper implements ExceptionMapper<Exception>
{
  private static final Logger log = LoggerFactory.getLogger(FilesExceptionMapper.class);

  @Override
  public Response toResponse(Exception exception)
  {
    TapisResponse<String> resp = TapisResponse.createErrorResponse(exception.getMessage());
    Response.Status status = INTERNAL_SERVER_ERROR;

    if (exception instanceof NotFoundException) status = NOT_FOUND;
    else if (exception instanceof NotAuthorizedException) status = UNAUTHORIZED;
    else if (exception instanceof ForbiddenException) status = FORBIDDEN;
    else if (exception instanceof BadRequestException) status = BAD_REQUEST;
    else if (exception instanceof WebApplicationException) status = INTERNAL_SERVER_ERROR;
    else log.error("UNCAUGHT_EXCEPTION", exception);

    return Response.status(status).type(MediaType.APPLICATION_JSON).entity(resp).build();
  }
}
