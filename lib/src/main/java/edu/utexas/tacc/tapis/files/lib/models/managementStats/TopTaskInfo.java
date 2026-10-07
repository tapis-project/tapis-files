package edu.utexas.tacc.tapis.files.lib.models.managementStats;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksRecord;

import java.time.Instant;

public class TopTaskInfo extends TaskInfo {
    public TopTaskInfo(int id, String status, String tenant, String user, Instant created) {
        super(id, status, tenant, user, created);
    }

}
