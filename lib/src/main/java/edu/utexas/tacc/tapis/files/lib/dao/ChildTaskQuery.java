package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksChildRecord;

import java.time.OffsetDateTime;
import java.util.UUID;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksChild.TRANSFER_TASKS_CHILD;

public class ChildTaskQuery extends FilesQueryBuilder<TransferTasksChildRecord> {
    public static SortableField<TransferTasksChildRecord, OffsetDateTime> SORT_FIELD_CREATED =
            new SortableField<TransferTasksChildRecord, OffsetDateTime>(TRANSFER_TASKS_CHILD.CREATED);

    public static final QueryCondition.ComparableField<TransferTasksChildRecord, Integer> COMPARE_FIELD_ID =
            new QueryCondition.ComparableField<>(TRANSFER_TASKS_CHILD.ID);
    public static final QueryCondition.ComparableField<TransferTasksChildRecord, String> COMPARE_FIELD_STATUS =
            new QueryCondition.ComparableField<>(TRANSFER_TASKS_CHILD.STATUS);
    public static final QueryCondition.ComparableField<TransferTasksChildRecord, Integer> COMPARE_FIELD_TOP_TASK_ID =
            new QueryCondition.ComparableField<>(TRANSFER_TASKS_CHILD.TASK_ID);
    public static final QueryCondition.ComparableField<TransferTasksChildRecord, UUID> COMPARE_FIELD_ASSIGNED_TO =
            new QueryCondition.ComparableField<>(TRANSFER_TASKS_CHILD.ASSIGNED_TO);

    public ChildTaskQuery() {
        super(TRANSFER_TASKS_CHILD);
    }
}
