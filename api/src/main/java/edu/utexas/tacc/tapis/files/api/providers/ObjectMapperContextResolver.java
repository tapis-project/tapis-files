package edu.utexas.tacc.tapis.files.api.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

  private final ObjectMapper mapper;

  public ObjectMapperContextResolver() {
    this.mapper = createObjectMapper();
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return mapper;
  }

  private ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    JavaTimeModule module = new JavaTimeModule();
    mapper.registerModule(module);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    return mapper;
  }
}