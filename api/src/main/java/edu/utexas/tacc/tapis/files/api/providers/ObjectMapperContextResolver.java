package edu.utexas.tacc.tapis.files.api.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.utexas.tacc.tapis.files.lib.json.TapisObjectMapper;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper>
{
  private final ObjectMapper mapper;

  public ObjectMapperContextResolver() {
    this.mapper = createObjectMapper();
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return mapper;
  }

  private ObjectMapper createObjectMapper()
  {
    ObjectMapper mapper = TapisObjectMapper.getMapper();
    return mapper;
  }
}