package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.lib.models.TransferTask;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskChild;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskParent;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

class TaskInfo {

    private int id;
    private String status;
    private String tenant;
    private String user;
    private Optional<String> errorMessage;
    private Instant created;

    TaskInfo(int id, String status, String tenant, String user, Instant created) {
        this.id = id;
        this.status = status;
        this.tenant = tenant;
        this.user = user;
        this.created = created;
        this.errorMessage = Optional.empty();
    }

    public void setErrorMessage(String errorMessage) {
        if (StringUtils.isBlank(errorMessage)) {
            this.errorMessage = Optional.empty();
        } else {
            this.errorMessage = Optional.of(errorMessage);
        }
    }

    public String getTenant() {
        return tenant;
    }

    public String getUser() {
        return user;
    }

    public Optional<String> getErrorMessage() {
        return errorMessage;
    }

    public Instant getCreated() {
        return created;
    }
}

