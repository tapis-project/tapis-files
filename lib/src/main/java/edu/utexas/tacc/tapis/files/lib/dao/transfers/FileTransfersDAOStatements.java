package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class FileTransfersDAOStatements {


    //language=SQL
    public static final String GET_TASK_BY_UUID =
        "SELECT * FROM transfer_tasks where uuid = ?";

    //language=SQL
    public static final String GET_TASK_BY_ID =
        "SELECT * FROM transfer_tasks where id = ?";

    //language=SQL
    public static final String GET_PARENT_TASK_BY_UUID =
        "SELECT * FROM transfer_tasks_parent where uuid = ?";

    //language=SQL
    public static final String GET_PARENTS_FOR_TASK_BY_ID =
        "SELECT * FROM transfer_tasks_parent where task_id = ?";

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
        "SELECT * FROM transfer_tasks where tenant_id = ? AND username = ? order by created DESC limit ? offset ?";

    //language=SQL
    public static final String GET_TRANSFER_TASK_SUMMARY_BY_UUID =
         """
         with tmp as (
             select
                 transfer_tasks.id as taskId,
                 ttc.bytes_transferred as bytes_transferred,
                 ttc.total_bytes as total_bytes,
                 ttc.status
             FROM transfer_tasks
             JOIN transfer_tasks_child ttc on transfer_tasks.id = ttc.task_id
             WHERE transfer_tasks.uuid = ?
        ) SELECT
         sum(tmp.total_bytes) as total_bytes,
         sum(tmp.bytes_transferred) as total_bytes_transferred,
         count(tmp.taskId) as total_transfers,
         count(tmp.taskId) FILTER ( WHERE  tmp.status = 'COMPLETED' ) as complete
         from tmp;
        """;

    //language=SQL
    public static final String GET_TRANSFER_TASK_SUMMARY_BY_ID =
        """
         with tmp as (
         select
             transfer_tasks.id as taskId,
             ttc.bytes_transferred as bytes_transferred,
             ttc.total_bytes as total_bytes,
             ttc.status
         FROM transfer_tasks
         JOIN transfer_tasks_child ttc on transfer_tasks.id = ttc.task_id
         WHERE transfer_tasks.id = ?
        ) SELECT
             sum(tmp.total_bytes) as total_bytes,
             sum(tmp.bytes_transferred) as total_bytes_transferred,
             count(tmp.taskId) as total_transfers,
             count(tmp.taskId) FILTER ( WHERE  tmp.status = 'COMPLETED' ) as complete
         from tmp;
        """;

    //language=SQL
    public static final String GET_TASK_FULL_HISTORY_BY_UUID =
        """
        SELECT * from 
            transfer_tasks as tasks 
            JOIN  transfer_tasks_parent as parents on parents.task_id = tasks.id
            JOIN transfer_tasks_child as children  on parents.id = children.parent_task_id
            WHERE tasks.uuid = ?
        """;

    //language=SQL
    public static final String UPDATE_TRANSFER_TASK =
        """
            UPDATE transfer_tasks 
             SET status = ?, 
                 start_time = ?, 
                 end_time = ?,
                 error_message = ?
            WHERE id = ? 
            RETURNING *
        """;


    //language=SQL
    public static final String UPDATE_CHILD_TASK =
        """
            UPDATE transfer_tasks_child
            SET bytes_transferred = ?, 
                     status = ?,
                     retries = ?, 
                     start_time = ?, 
                     end_time = ?,
                     error_message = ?
                WHERE id = ? 
                RETURNING *
        """;

    //language=SQL
    public static final String GET_CHILD_TASK_INCOMPLETE_COUNT =
        "SELECT count(id) from transfer_tasks_child " +
            "WHERE task_id = ? " +
            "AND status != 'COMPLETED' ";

    //language=SQL
    public static final String GET_CHILD_TASK_INCOMPLETE_COUNT_FOR_PARENT =
        "SELECT count(id) from transfer_tasks_child " +
            "WHERE parent_task_id = ? " +
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
            "RETURNING *";

    //language=SQL
    public static final String UPDATE_PARENT_TASK =
        """
            UPDATE transfer_tasks_parent 
                     SET source_uri = ?, 
                         destination_uri = ?, 
                         status = ?, 
                         start_time = ?, 
                         end_time = ?, 
                         bytes_transferred =?, 
                         total_bytes = ?,
                         error_message = ?
                    WHERE uuid = ? 
                    RETURNING *
        """;


    //language=SQL
    public static final String INSERT_TASK =
        "INSERT into transfer_tasks " +
            "(tenant_id, username, status, tag)" +
            "values (?, ?, ?, ?)" +
            "RETURNING *";

    //language=SQL
    public static final String INSERT_PARENT_TASK =
        "INSERT into transfer_tasks_parent " +
            "(tenant_id, task_id, username, source_uri, destination_uri, status, optional)" +
            "values (?, ?, ?, ?, ?, ?, ?)" +
            "RETURNING *";

    //language=SQL
    public static final String INSERT_CHILD_TASK =
        "INSERT into transfer_tasks_child " +
            " (tenant_id, task_id, parent_task_id, username, source_uri, destination_uri, status, bytes_transferred, total_bytes) " +
            " values (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            " RETURNING * ";

    //language=SQL
    public static final String CANCEL_TRANSFER_TASK_AND_CHILDREN =
        """
            UPDATE transfer_tasks set status = 'CANCELLED' where id = ?;
            UPDATE transfer_tasks_parent set status = 'CANCELLED' where task_id = ?;
            UPDATE transfer_tasks_child set status = 'CANCELLED' where task_id = ?;
        """;

    //language=SQL
    public static final String UPDATE_CHILD_TASK_BYTES_TRANSFERRED =
        """
            UPDATE transfer_tasks_child set bytes_transferred = ? WHERE id = ?
        """;


}
