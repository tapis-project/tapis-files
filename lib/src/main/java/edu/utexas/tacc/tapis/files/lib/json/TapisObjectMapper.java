package edu.utexas.tacc.tapis.files.lib.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;


/**
 * This class is a singleton for getting the default object mapper for the files service. This way, we can
 * use the mapper the web api and also when serializing objects in rabbitmq etc.
 */
public class TapisObjectMapper {

    private static ObjectMapper mapper;


    /**
     * gets/creates the object mapper and configures it for datetimes etc.
     * @return ObjectMapper
     */
    public static ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            return mapper;
        }
        return mapper;
    }

}
