package edu.utexas.tacc.tapis.files.api.responses;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that can be used on DTO classes.  Used by DTOResponseBuilder to
 * return only select attributes of the DTO.  Annotations should be placed on
 * the "getter" methods for properties that can be returned.  By default, an
 * attribute is considered a "summaryAttribute", but if summaryAttribute is
 * set to false, then it will only be returned if it is specifically requested
 * or if "allAttributes" is requested.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DTOProperty {
    public boolean summaryAttribute() default false;
}
