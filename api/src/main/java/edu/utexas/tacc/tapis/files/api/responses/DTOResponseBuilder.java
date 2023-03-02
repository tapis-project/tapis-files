package edu.utexas.tacc.tapis.files.api.responses;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds a response that returns selected attributes.  Those attributes can
 * have the special values "allAttributes" or "summaryAttributes".  If allAttributes
 * is requested, than all properties in the DTO with the annotation DTOProperty
 * on the getter or setter will be returned.  If "summaryAttributes" is requested,
 * only those properties with the summaryAttributes set to true (or left blank since
 * it's the default value) will be returned.  Individual attributes can be specified
 * also - for example id, name, owner, etc.  If summaryAttributes is provided with
 * other attributes - for example summaryAttributes,myvalue - the additional properties
 * will be combinded with the summary set.
 * @param <DT> - the DTO class (for example PostItsDTO.class)
 */
public class DTOResponseBuilder<DT> {

    Logger log = LoggerFactory.getLogger(DTOResponseBuilder.class);
    public static final boolean PRETTY = true;

    private static final String ALL_ATTRIBUTES = "allAttributes";
    private static final String SUMMARY_ATTRIBUTES = "summaryAttributes";

    private class PropertyInfo {
        private final DTOProperty dtoProperty;
        private final PropertyDescriptor propertyDescriptor;

        public PropertyInfo(PropertyDescriptor propertyDescriptor, DTOProperty dtoProperty) {
            this.propertyDescriptor = propertyDescriptor;
            this.dtoProperty = dtoProperty;
        }

        public DTOProperty getDtoProperty() {
            return dtoProperty;
        }

        public PropertyDescriptor getPropertyDescriptor() {
            return propertyDescriptor;
        }
    }

    public static class DTOResponse extends RespAbstract {
        private final JsonElement result;

        DTOResponse(JsonElement result) {
            this.result = result;
        }
        public JsonElement getContainedObject() {
            return result;
        }
    }

    Map<String, PropertyInfo> propertyDescriptorMap = new HashMap<>();
    Set<String> selectedProperties = null;

    /**
     * Initializes the response builder.  This sets the fields to be returned.  It relies on
     * reflection to get the annotations, and process the select list.
     *
     * @param dtoClass The class for the dto
     * @param selectList The list of included attributes (can be allAttributes or summaryAttributes)
     * @throws TapisException
     */
    public DTOResponseBuilder(Class<DT> dtoClass, List<String> selectList) throws TapisException {
        populateMap(dtoClass);
        if (CollectionUtils.isEmpty(selectList)) {
            selectedProperties = getSummaryAttributes();
        } else {
            selectedProperties = new HashSet<>();
            for(String property : selectList) {
                if(property.equals(ALL_ATTRIBUTES)) {
                    // no need to continue, they selected all
                    selectedProperties.addAll(propertyDescriptorMap.keySet());
                    break;
                } else if(property.equals(SUMMARY_ATTRIBUTES)) {
                    selectedProperties = getSummaryAttributes();
                } else {
                    selectedProperties.add(property);
                }
            }
        }
    }

    /**
     * Builds a response object with an array of dto's.  Each dto will contain only the
     * selected fields.
     *
     * @param status Http Status
     * @param message Message for the response
     * @param dtos List of dto's to return
     * @return the new response message
     */
    public Response createSuccessResponse(Status status, String message, List<DT> dtos) {
        JsonArray result = new JsonArray();

        if(dtos != null) {
            for (DT dto : dtos) {
                result.add(getJsonObject(dto));
            }
        }

        return createSuccessResponse(status, message, result);
    }

    /**
     * Builds a response object with dto as the result.  The dto will contain only the
     * selected fields.
     *
     * @param status Http Status
     * @param message Message for the response
     * @param dto The dto to return
     * @return the new response message
     */
    public Response createSuccessResponse(Status status, String message, DT dto) {
        return createSuccessResponse(status, message, getJsonObject(dto));
    }

    private Set<String> getSummaryAttributes() {
        return propertyDescriptorMap.keySet().stream().filter(key -> {
            DTOProperty dtoProperty = propertyDescriptorMap.get(key).getDtoProperty();
            return dtoProperty.summaryAttribute();
        }).collect(Collectors.toSet());
    }

    private JsonObject getJsonObject(DT dto) {
        JsonObject returnValue = new JsonObject();
        if (dto == null) {
            return returnValue;
        }

        try {
            for(String fieldName : selectedProperties) {
                addField(returnValue, dto, fieldName);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }

        return returnValue;
    }

    private void addField(JsonObject object, DT dto, String propertyName)
            throws IllegalAccessException, InvocationTargetException, TapisException {
        PropertyInfo propertyInfo = propertyDescriptorMap.get(propertyName);

        if(propertyInfo == null) {
            String className = (dto == null) ? null : dto.getClass().getCanonicalName();
            String msg = LibUtils.getMsg("POSTITS_ERROR_UNKNOWN_FIELD", propertyName, className);
            throw new TapisException(msg);
        }

        DTOProperty dtoProperty = propertyInfo.getDtoProperty();
        PropertyDescriptor propertyDescriptor = propertyInfo.getPropertyDescriptor();

        String name = propertyDescriptor.getName();

        Class<?> type = propertyDescriptor.getPropertyType();
        if(Number.class.isAssignableFrom(type)) {
            object.addProperty(name, getProperty(dto, propertyDescriptor.getReadMethod(), Number.class));
        } else if(Boolean.class.isAssignableFrom(type)) {
            object.addProperty(name, getProperty(dto, propertyDescriptor.getReadMethod(), Boolean.class));
        } else if(Character.class.isAssignableFrom(type)) {
            object.addProperty(name, getProperty(dto, propertyDescriptor.getReadMethod(), Character.class));
        } else if(String.class.isAssignableFrom(type)) {
            object.addProperty(name, getProperty(dto, propertyDescriptor.getReadMethod(), String.class));
        } else {
            Object value = getProperty(dto, propertyDescriptor.getReadMethod(), Object.class);
            String valueString = (value == null) ? null : value.toString();
            object.addProperty(name, valueString);
        }
    }

    private <PT> PT getProperty(DT dto, Method method, Class<PT> propertyType)
            throws InvocationTargetException, IllegalAccessException {
        PT propertyValue = null;

        if(method != null) {
            propertyValue = (PT)method.invoke(dto);
        }

        return propertyValue;
    }

    private void populateMap(Class<DT> dtoClass) throws TapisException {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(dtoClass);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            if (propertyDescriptors != null) {
                for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                    // first look for an annotation on the read method
                    DTOProperty dtoProperty = null;
                    Method readMethod = propertyDescriptor.getReadMethod();
                    if (readMethod != null) {
                        dtoProperty = readMethod.getAnnotation(DTOProperty.class);
                    }

                    // if none found, check the write method
                    if (dtoProperty == null) {
                        Method writeMethod = propertyDescriptor.getWriteMethod();
                        if (writeMethod != null) {
                            dtoProperty = writeMethod.getAnnotation(DTOProperty.class);
                        }
                    }

                    // ignore any properties that have no annotation on either the getter or setter
                    if(dtoProperty != null) {
                        PropertyInfo propertyInfo = new PropertyInfo(propertyDescriptor, dtoProperty);
                        propertyDescriptorMap.put(propertyDescriptor.getName(), propertyInfo);
                    }
                }
            }
        } catch (Exception ex) {
            String msg = LibUtils.getMsg("POSTITS_INTROSPECTION_ERROR", ex.getMessage());
            throw new TapisException(msg, ex);
        }
    }

    private Response createSuccessResponse(Status status, String message, JsonElement jsonElement) {
        DTOResponse dtoResponse = new DTOResponse(jsonElement);
        return Response.status(status).entity(TapisRestUtils.createSuccessResponse(message, PRETTY, dtoResponse)).build();
    }
}
