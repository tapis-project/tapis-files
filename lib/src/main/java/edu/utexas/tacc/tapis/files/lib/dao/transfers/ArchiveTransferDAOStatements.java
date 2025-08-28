package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class ArchiveTransferDAOStatements {
    public static String INSERT_ARCHIVE_TRANSFER=
            """
                INSERT INTO archive_transfers (username, tenant_id, 
                    status, source_base_url, destination_base_url,
                    archive_type, archive_bytes_read, file_bytes_read, error_message,
                    src_shared_ctx, dst_shared_ctx, retries_remaining) VALUES 
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING *;
            """;

    public static String INSERT_RELATIVE_PATHS=
            """
                INSERT INTO archive_transfer_paths (archive_transfer_id, path) VALUES (?, ?)
                    RETURNING path;
            """;

    public static final String GET_ACCEPTED_AND_RETRY_ARCHIVE_TRANSFERS_FOR_TENANTS_AND_USERS =
            """
              SELECT * FROM (
                select
                  *, row_number() OVER (
                    PARTITION BY
                      tenant_id,
                      username
                    order by
                      created
                  )
                from
                  archive_transfers
                where
                  assigned_to IS NULL  AND 
                  ((status = 'ACCEPTED') OR 
                    (status = 'AWAITING_RETRY' 
                    AND next_retry <= NOW()))
              )
                where
                  row_number <= ?
                order by
                  row_number;
            """ ;

    public static final String GET_ACCEPTED_AND_RETRY_ARCHIVE_TRANSFERS_ASSIGNED_TO_WORKER =
            """
            SELECT * from (
              SELECT *, row_number() 
              OVER (
                PARTITION BY 
                  tenant_id, 
                  username 
                ORDER BY created) 
              FROM archive_transfers 
              WHERE 
                assigned_to = ? AND
                ((status = 'ACCEPTED') OR 
                (status = 'AWAITING_RETRY' 
                AND next_retry <= NOW()))
              )
            WHERE row_number <= ? ORDER BY row_number;
            """;

    public static final String GET_ASSIGNED_ARCHIVE_TRANSFER_COUNT =
            """
              SELECT
                  assigned_to, count(*)
              FROM
                  archive_transfers at
                  INNER JOIN transfer_worker tw on at.assigned_to=tw.uuid
              group by
                  assigned_to;
            """ ;

    public static final String ASSIGN_TASKS_TO_WORKER =
            """
              UPDATE
                  archive_transfers 
              SET
                  assigned_to = ?
              WHERE
                  id = ANY(?);
            """ ;

    public static final String GET_RELATIVE_PATHS_FOR_ID =
            """
            SELECT path from archive_transfer_paths where archive_transfer_id = ?;
            """;

    public static final String GET_ARCHIVE_LOG_FOR_ID =
            """
            SELECT log from archive_transfer_log where archive_transfer_id = ?;
            """;

    public static String INSERT_TRANSFER_LOG =
            """
                INSERT INTO archive_transfer_log (archive_transfer_id, log) VALUES (?, ?)
                    RETURNING log;
            """;

    public static final String UNASSIGN_ZOMBIE_ASSIGNMENTS =
            """
              UPDATE
                  archive_transfers
              SET
                  assigned_to = null
              WHERE id IN (
                  SELECT
                      id
                  FROM
                      archive_transfers at left join transfer_worker tw on at.assigned_to = tw."uuid"
                  WHERE
                      at.assigned_to is not null and tw."uuid" is null
              ) AND
                  archive_transfers.status != ANY(?)
            """ ;
    public static final String GET_ARCHIVE_TRANSFER_FOR_UPDATE =
            """
              SELECT
                  *
              FROM
                  archive_transfers
              WHERE uuid = ?
              FOR update
            """ ;
    public static final String GET_ARCHIVE_TRANSFER =
            """
              SELECT
                  *
              FROM
                  archive_transfers
              WHERE uuid = ?
            """ ;
    public static final String UPDATE_ARCHIVE_TRANSFER =
            """
              UPDATE
                  archive_transfers 
              SET
                  status = ?,
                  error_message = ?,
                  archive_bytes_read = ?,
                  file_bytes_read = ?,
                  retries_remaining = ?,
                  next_retry = ?,
                  start_time = ?,
                  end_time = ?,
                  assigned_to = ?
              WHERE
                  uuid = ?
              RETURNING 
                  *         
            """ ;

}