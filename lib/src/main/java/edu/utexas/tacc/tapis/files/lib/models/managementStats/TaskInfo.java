package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

class TaskInfo {

    private int id;
    private UUID uuid;
    private String status;
    private String tenant;
    private String user;
    private String errorMessage;
    private Instant created;

    TaskInfo(int id, UUID uuid, String status, String tenant, String user, Instant created) {
        this.id = id;
        this.uuid = uuid;
        this.status = status;
        this.tenant = tenant;
        this.user = user;
        this.created = created;
    }

    public int getId() {
        return id;
    }

    public String getTenant() {
        return tenant;
    }

    public String getUser() {
        return user;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = StringUtils.isBlank(errorMessage) ? null : errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Instant getCreated() {
        return created;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }
}

