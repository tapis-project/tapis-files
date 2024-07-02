package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class TransferWorkerDAOStatements {
    public static final String INSERT_TRANSFER_WORKER =
        """
            INSERT INTO transfer_worker DEFAULT VALUES
                RETURNING *;
        """;

    public static final String SELECT_TRANSFER_WORKER_BY_UUID =
            """
                SELECT * from transfer_worker WHERE UUID = ?;
            """;
    public static final String DELETE_TRANSFER_WORKER_BY_UUID =
            """
                DELETE from transfer_worker WHERE UUID = ? RETURNING *;
            """;
    public static final String SELECT_ALL_TRANSFER_WORKERS =
            """
                SELECT * from transfer_worker;
            """;
    public static final String UPDATE_TRANSFER_WORKER_TIME =
            """
                UPDATE transfer_worker set last_updated = DEFAULT WHERE UUID = ? RETURNING *;
            """;
}
