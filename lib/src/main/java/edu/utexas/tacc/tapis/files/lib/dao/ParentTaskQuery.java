package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.TransferTasksParentRecord;

import java.time.OffsetDateTime;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksParent.TRANSFER_TASKS_PARENT;

public class ParentTaskQuery extends FilesQueryBuilder<TransferTasksParentRecord> {
    public static SortableField<TransferTasksParentRecord, Integer> SORT_FIELD_ID =
            new SortableField<TransferTasksParentRecord, Integer>(TRANSFER_TASKS_PARENT.ID);
    public static SortableField<TransferTasksParentRecord, OffsetDateTime> SORT_FIELD_CREATED =
            new SortableField<TransferTasksParentRecord, OffsetDateTime>(TRANSFER_TASKS_PARENT.CREATED);

    public static final ComparableField<TransferTasksParentRecord, Integer> COMPARE_FIELD_ID =
            new ComparableField<TransferTasksParentRecord, Integer>(TRANSFER_TASKS_PARENT.ID);
    public static final ComparableField<TransferTasksParentRecord, Integer> COMPARE_FIELD_TOP_TASK_ID =
            new ComparableField<TransferTasksParentRecord, Integer>(TRANSFER_TASKS_PARENT.TASK_ID);
    public static final ComparableField<TransferTasksParentRecord, String> COMPARE_FIELD_STATUS =
            new ComparableField<TransferTasksParentRecord, String>(TRANSFER_TASKS_PARENT.STATUS);

    public ParentTaskQuery() {
        super(TRANSFER_TASKS_PARENT);
    }
}
