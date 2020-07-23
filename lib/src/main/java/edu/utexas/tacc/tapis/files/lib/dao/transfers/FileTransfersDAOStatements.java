package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class FileTransfersDAOStatements {

    //language=SQL
    public static final String GET_PARENT_TASK_BY_UUID =
        "SELECT * FROM transfer_tasks where uuid = ?";

    //language=SQL
    public static final String GET_PARENT_TASK_BY_ID =
        "SELECT * FROM transfer_tasks where id = ?";

    //language=SQL
    public static final String GET_CHILD_TASK_BY_ID =
        "SELECT * FROM transfer_tasks_child where uuid = ?";

    //language=SQL
    public static final String GET_ALL_CHILDREN =
        "SELECT * FROM transfer_tasks_child where parent_task_id = ?";

    //language=SQL
    public static final String GET_ALL_TASKS_FOR_USER =
        "SELECT * FROM transfer_tasks where tenant_id = ? AND username = ?";

    //language=SQL
    public static final String UPDATE_CHILD_TASK =
        "UPDATE transfer_tasks_child " +
            " SET bytes_transferred = ?, " +
            "     status = ? " +
            "WHERE id = ? " +
            "RETURNING transfer_tasks_child.*";

    //language=SQL
    public static final String GET_CHILD_TASK_INCOMPLETE_COUNT =
        "SELECT count(id) from transfer_tasks_child " +
            "WHERE parent_task_id = ? " +
            "AND status != 'FAILED' ";


    //language=SQL
    public static final String UPDATE_PARENT_TASK_SIZE =
        "UPDATE transfer_tasks " +
            "SET total_bytes = total_bytes + ?" +
            "WHERE id = ? " +
            "RETURNING transfer_tasks.*";

    //language=SQL
    public static final String UPDATE_PARENT_TASK_BYTES_TRANSFERRED =
        "UPDATE transfer_tasks " +
            "SET bytes_transferred = bytes_transferred + ?" +
            "WHERE id = ? " +
            "RETURNING transfer_tasks.*";

    //language=SQL
    public static final String UPDATE_PARENT_TASK =
        "UPDATE transfer_tasks " +
            " SET source_system_id = ?, " +
            "     source_path = ?, " +
            "     destination_system_id = ?, " +
            "     destination_path = ?, " +
            "     status = ? " +
            "WHERE uuid = ? " +
            "RETURNING transfer_tasks.*";

    //language=SQL
    public static final String INSERT_PARENT_TASK =
        "INSERT into transfer_tasks " +
            "(uuid, tenant_id, username, source_system_id, source_path, destination_system_id, destination_path, status)" +
            "values (?, ?, ?, ?, ?, ?, ?, ?)" +
            "RETURNING transfer_tasks.*";

    //language=SQL
    public static final String INSERT_CHILD_TASK =
        "INSERT into transfer_tasks_child " +
            " (tenant_id, parent_task_id, username, source_system_id, source_path, destination_system_id, destination_path, status, bytes_transferred, total_bytes) " +
            " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            " RETURNING transfer_tasks_child.* ";


}
