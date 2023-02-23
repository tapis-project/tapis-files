package edu.utexas.tacc.tapis.files.api.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Update PostIt request
 */
public class PostItUpdateRequest {

    // Number of times the PostIt may be redeemed
    @JsonProperty("allowedUses")
    private Integer allowedUses;

    // Number of seconds until the PostIt expires
    @JsonProperty("validSeconds")
    private Integer validSeconds;

    public Integer getAllowedUses() {
        return allowedUses;
    }

    public void setAllowedUses(Integer allowedUses) {
        this.allowedUses = allowedUses;
    }

    public Integer getValidSeconds() {
        return validSeconds;
    }

    public void setValidSeconds(Integer validSeconds) {
        this.validSeconds = validSeconds;
    }
}