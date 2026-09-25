package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import java.time.Instant;
import java.util.Objects;

public class TransferWorkerInfo {
    private String uuid;
    private Instant last_updated;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public Instant getLast_updated() {
        return last_updated;
    }

    public void setLast_updated(Instant last_updated) {
        this.last_updated = last_updated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransferWorkerInfo that)) return false;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }
}
