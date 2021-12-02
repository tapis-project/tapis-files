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

public class FilesExceptionMapper implements ExceptionMapper<Exception>
{
  private static final Logger log = LoggerFactory.getLogger(FilesExceptionMapper.class);

  @Override
  public Response toResponse(Exception exception)
  {
    TapisResponse<String> resp = TapisResponse.createErrorResponse(exception.getMessage());

    if (exception instanceof NotFoundException) {
      return Response.status(Response.Status.NOT_FOUND)
              .type(MediaType.APPLICATION_JSON)
              .entity(resp).build();
    } else if (exception instanceof NotAuthorizedException || exception instanceof ForbiddenException) {
      return Response.status(Response.Status.FORBIDDEN)
              .type(MediaType.APPLICATION_JSON)
              .entity(resp).build();
    } else if (exception instanceof BadRequestException) {
      return Response.status(Response.Status.BAD_REQUEST)
              .type(MediaType.APPLICATION_JSON)
              .entity(resp).build();
    } else if (exception instanceof WebApplicationException) {
      log.error("??????????????UNCAUGHT_EXCEPTION", exception);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .type(MediaType.APPLICATION_JSON)
              .entity(resp).build();
    } else {
      log.error("UNCAUGHT_EXCEPTION", exception);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
              .type(MediaType.APPLICATION_JSON)
              .entity(resp).build();
    }
  }
}
