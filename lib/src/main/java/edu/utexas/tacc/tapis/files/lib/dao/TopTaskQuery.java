package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksRecord;

import java.time.OffsetDateTime;
import java.util.UUID;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasks.TRANSFER_TASKS;

public class TopTaskQuery extends FilesQueryBuilder<TransferTasksRecord> {
    public static SortableField<TransferTasksRecord, OffsetDateTime> SORT_FIELD_CREATED =
            new SortableField<TransferTasksRecord, OffsetDateTime>(TRANSFER_TASKS.CREATED);

    public static final ComparableField<TransferTasksRecord, Integer> COMPARE_FIELD_ID =
            new ComparableField<TransferTasksRecord, Integer>(TRANSFER_TASKS.ID);
    public static final ComparableField<TransferTasksRecord, UUID> COMPARE_FIELD_UUID =
            new ComparableField<TransferTasksRecord, UUID>(TRANSFER_TASKS.UUID);
    public static final ComparableField<TransferTasksRecord, String> COMPARE_FIELD_STATUS =
            new ComparableField<TransferTasksRecord, String>(TRANSFER_TASKS.STATUS);

    public TopTaskQuery() {
        super(TRANSFER_TASKS);
    }
}
