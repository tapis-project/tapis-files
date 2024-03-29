package edu.utexas.tacc.tapis.files.lib.models;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;

public class PostIt {
    // Id for the PostIt (used for redeeming)
    private String id;
    // Id of the system containing the path
    private String systemId;
    // Path relative to home directory of system
    private String path;
    // Total number of times the PostIt may be retrieved
    private Integer allowedUses;
    // Total number of times the PostIt has already been retrieved
    private Integer timesUsed;
    // jwtUser from the create request
    private String jwtUser;
    // jwtTenant from the create request
    private String jwtTenantId;
    // Owner of the PostIt (used for redeem permission checks)
    private String owner;

    // TenantId of the PostIt (used for redeem permission checks)
    private String tenantId;
    // Constructed field containing the url used to redeem the PostIt (no authentication needed)
    private Instant expiration;
    // Date/time of creation
    private Instant created;
    // Date/time of last update
    private Instant updated;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getAllowedUses() {
        return allowedUses;
    }

    public void setAllowedUses(Integer allowedUses) {
        this.allowedUses = allowedUses;
    }

    public Integer getTimesUsed() {
        return timesUsed;
    }

    public void setTimesUsed(Integer timesUsed) {
        this.timesUsed = timesUsed;
    }

    public String getJwtUser() {
        return jwtUser;
    }

    public void setJwtUser(String jwtUser) {
        this.jwtUser = jwtUser;
    }

    public String getJwtTenantId() {
        return jwtTenantId;
    }

    public void setJwtTenantId(String jwtTenantId) {
        this.jwtTenantId = jwtTenantId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Instant getExpiration() {
        return expiration;
    }

    public void setExpiration(Instant expiration) {
        this.expiration = expiration;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("PostIt");
        builder.append(System.lineSeparator());
        builder.append("  Id: ").append(getId()).append(System.lineSeparator());
        builder.append("  systemId: ").append(getSystemId()).append(System.lineSeparator());
        builder.append("  path: ").append(getPath()).append(System.lineSeparator());
        builder.append("  allowedUses: ").append(getAllowedUses()).append(System.lineSeparator());
        builder.append("  expiration: ").append(getExpiration()).append(System.lineSeparator());
        builder.append("  jwtUser: ").append(getJwtUser()).append(System.lineSeparator());
        builder.append("  jwtTenantId: ").append(getJwtTenantId()).append(System.lineSeparator());
        builder.append("  owner: ").append(getOwner()).append(System.lineSeparator());
        builder.append("  tenantId: ").append(getTenantId()).append(System.lineSeparator());
        builder.append("  timesUsed: ").append(getTimesUsed()).append(System.lineSeparator());
        builder.append("  created: ").append(getCreated()).append(System.lineSeparator());
        builder.append("  updated: ").append(getUpdated()).append(System.lineSeparator());
        return builder.toString();
    }

    @JsonIgnore
    public boolean isRedeemable() {
        // if it's expired based on the time, it's not redeemable
        if(Instant.now().isAfter(this.getExpiration())) {
            return false;
        }

        // if its not unlimited uses, and the number of uses is greater or equal
        // to the allowed uses, it's also not redeemable
        if ( (this.getAllowedUses() != -1) &&
                (this.getAllowedUses() <= this.getTimesUsed()) ) {
            return false;
        }

        return true;
    }
}
