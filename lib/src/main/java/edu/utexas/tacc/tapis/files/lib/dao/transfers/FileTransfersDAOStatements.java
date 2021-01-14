package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class FileTransfersDAOStatements {


    //language=SQL
    public static final String GET_TASK_BY_UUID =
        "SELECT * FROM transfer_tasks where uuid = ?";

    //language=SQL
    public static final String GET_PARENT_TASK_BY_UUID =
        "SELECT * FROM transfer_tasks where uuid = ?";

    //language=SQL
    public static final String GET_PARENT_TASK_BY_ID =
        "SELECT * FROM transfer_tasks_parent where id = ?";

    //language=SQL
    public static final String GET_CHILD_TASK_BY_UUID =
        "SELECT * FROM transfer_tasks_child where uuid = ?";

    //language=SQL
    public static final String GET_ALL_CHILDREN_FOR_PARENT =
        "SELECT * FROM transfer_tasks_child where parent_task_id = ?";

    //language=SQL
    public static final String GET_ALL_CHILDREN =
        "SELECT * FROM transfer_tasks_child where task_id = ?";

    //language=SQL
    public static final String GET_ALL_TASKS_FOR_USER =
        "SELECT * FROM transfer_tasks where tenant_id = ? AND username = ?";

    //language=SQL
    public static final String UPDATE_TRANSFER_TASK =
        "UPDATE transfer_tasks " +
            " SET status = ?, " +
            "     start_time = ?, " +
            "     end_time = ? " +
            "WHERE id = ? " +
            "RETURNING *";

    //language=SQL
    public static final String UPDATE_CHILD_TASK =
        "UPDATE transfer_tasks_child " +
            " SET bytes_transferred = ?, " +
            "     status = ?, " +
            "     retries = ? " +
            "WHERE id = ? " +
            "RETURNING *";

    //language=SQL
    public static final String GET_CHILD_TASK_INCOMPLETE_COUNT =
        "SELECT count(id) from transfer_tasks_child " +
            "WHERE task_id = ? " +
            "AND status != 'COMPLETED' ";


    //language=SQL
    public static final String UPDATE_PARENT_TASK_SIZE =
        "UPDATE transfer_tasks_parent " +
            "SET total_bytes = total_bytes + ?" +
            "WHERE id = ? " +
            "RETURNING *";

    //language=SQL
    public static final String UPDATE_PARENT_TASK_BYTES_TRANSFERRED =
        "UPDATE transfer_tasks_parent " +
            "SET bytes_transferred = bytes_transferred + ?" +
            "WHERE id = ? " +
            "RETURNING transfer_tasks_parent.*";

    //language=SQL
    public static final String UPDATE_PARENT_TASK =
        "UPDATE transfer_tasks_parent " +
            " SET source_uri = ?, " +
            "     destination_uri = ?, " +
            "     status = ?, " +
            "     total_bytes = ? " +
            "WHERE uuid = ? " +
            "RETURNING *";

    //language=SQL
    public static final String INSERT_TASK =
        "INSERT into transfer_tasks " +
            "(tenant_id, username, status, tag)" +
            "values (?, ?, ?, ?)" +
            "RETURNING *";

    //language=SQL
    public static final String INSERT_PARENT_TASK =
        "INSERT into transfer_tasks_parent " +
            "(tenant_id, task_id, username, source_uri, destination_uri, status)" +
            "values (?, ?, ?, ?, ?, ?)" +
            "RETURNING *";

    //language=SQL
    public static final String INSERT_CHILD_TASK =
        "INSERT into transfer_tasks_child " +
            " (tenant_id, task_id, parent_task_id, username, source_uri, destination_uri, status, bytes_transferred, total_bytes) " +
            " values (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            " RETURNING * ";


}
