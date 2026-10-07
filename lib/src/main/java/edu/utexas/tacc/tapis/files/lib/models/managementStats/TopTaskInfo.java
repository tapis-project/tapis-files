package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksRecord;

import java.time.Instant;
import java.util.UUID;

public class TopTaskInfo extends TaskInfo {
    public TopTaskInfo(int id, UUID uuid, String status, String tenant, String user, Instant created) {
        super(id, uuid, status, tenant, user, created);
    }

}
