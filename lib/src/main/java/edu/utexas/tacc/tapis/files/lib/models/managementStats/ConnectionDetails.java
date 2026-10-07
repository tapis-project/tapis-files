package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.shared.ssh.stats.SessionPoolDetails;

import java.time.Instant;

public class ConnectionDetails {
    String contextName;
    SessionPoolDetails sessionPoolDetails;
    Instant created;
    Instant updated;

    public String getContextName() {
        return contextName;
    }

    public SessionPoolDetails getSessionPoolDetails() {
        return sessionPoolDetails;
    }

    public Instant getCreated() {
        return created;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setContextName(String contextName) {
        this.contextName = contextName;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public void setSessionPoolDetails(SessionPoolDetails sessionPoolDetails) {
        this.sessionPoolDetails = sessionPoolDetails;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }
}
