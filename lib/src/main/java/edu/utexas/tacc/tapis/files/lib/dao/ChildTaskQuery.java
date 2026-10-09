package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksChildRecord;

import java.time.OffsetDateTime;
import java.util.UUID;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksChild.TRANSFER_TASKS_CHILD;

public class ChildTaskQuery extends FilesQueryBuilder<TransferTasksChildRecord> {
    public static SortableField<TransferTasksChildRecord, OffsetDateTime> SORT_FIELD_CREATED =
            new SortableField<TransferTasksChildRecord, OffsetDateTime>(TRANSFER_TASKS_CHILD.CREATED);

    public static final ComparableField<TransferTasksChildRecord, Integer> COMPARE_FIELD_ID =
            new ComparableField<TransferTasksChildRecord, Integer>(TRANSFER_TASKS_CHILD.ID);
    public static final ComparableField<TransferTasksChildRecord, String> COMPARE_FIELD_STATUS =
            new ComparableField<TransferTasksChildRecord, String>(TRANSFER_TASKS_CHILD.STATUS);
    public static final ComparableField<TransferTasksChildRecord, Integer> COMPARE_FIELD_TOP_TASK_ID =
            new ComparableField<TransferTasksChildRecord, Integer>(TRANSFER_TASKS_CHILD.TASK_ID);
    public static final ComparableField<TransferTasksChildRecord, UUID> COMPARE_FIELD_ASSIGNED_TO =
            new ComparableField<TransferTasksChildRecord, UUID>(TRANSFER_TASKS_CHILD.ASSIGNED_TO);

    public ChildTaskQuery() {
        super(TRANSFER_TASKS_CHILD);
    }
}
