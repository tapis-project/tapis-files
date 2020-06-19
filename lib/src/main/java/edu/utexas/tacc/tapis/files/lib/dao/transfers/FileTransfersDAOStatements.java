package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class FileTransfersDAOStatements {

    //language=SQL
    public static final String GET_PARENT_TASK_BY_ID =
        "SELECT * FROM transfer_tasks where uuid = ?";

    //language=SQL
    public static final String GET_CHILD_TASK_BY_ID =
        "SELECT * FROM transfer_tasks_child where uuid= ?";

    //language=SQL
    public static final String UPDATE_PARENT_TASK_SIZE =
        "UPDATE transfer_tasks " +
            "SET total_bytes += ?" +
            "WHERE uuid = ? " +
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
