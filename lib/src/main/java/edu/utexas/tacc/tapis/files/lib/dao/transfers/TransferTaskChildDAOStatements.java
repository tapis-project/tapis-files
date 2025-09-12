package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class TransferTaskChildDAOStatements {
    public static final String GET_ACCEPTED_CHILD_TASKS_FOR_TENANTS_AND_USERS =
            """
              select * from (
                select
                  *, row_number() over (
                    partition by
                      tenant_id,
                      username
                    order by
                      created
                  )
                from
                  transfer_tasks_child
                where
                  status = 'ACCEPTED' AND
                  assigned_to IS NULL
              )
                where
                  row_number <= ?
                order by
                  row_number;
            """ ;

    // NOTE that this contains CANCELLED tasks also.  The worker MUST check the
    // status and discard any cancelled tasks.  The assigner doesn't know if the
    // worker was working on this task yet when it was cancelled, or it it was just
    // in the queue and just not picked up yet.  Or it could even have been picked up
    // but just not set to in_progress yet.  For all of these reasonse, the assigner
    // cant know, so the worker MUST check status, and discard and unassigne cancelled
    // tasks. (this goes for parent tasks too!
    public static final String GET_ACCEPTED_CHILD_TASKS_ASSIGNED_TO_WORKER =
            """
              select * from (
                select
                  *, row_number() over (
                    partition by
                      tenant_id,
                      username
                    order by
                      created
                  )
                from
                  transfer_tasks_child
                where
                  (status = 'ACCEPTED' OR
                  status = 'CANCELLED') AND
                  assigned_to = ?
              )
                where
                  row_number <= ?
                order by
                  row_number;
            """ ;


    public static final String GET_ASSIGNED_CHILD_COUNT =
            """
              select
                  assigned_to, count(*)
              from
                  transfer_tasks_child ttc
                  inner join transfer_worker tw on ttc.assigned_to=tw.uuid
              group by
                  assigned_to;
            """ ;

    public static final String ASSIGN_TASKS_TO_WORKER =
            """
              update
                  transfer_tasks_child ttc
              set
                  assigned_to = ?
              where
                  id = ANY(?);
            """ ;

    public static final String UNASSIGN_ZOMBIE_ASSIGNMENTS =
            """
              update
                  transfer_tasks_child
              set
                  assigned_to = null
              where id in (
                  select
                      id
                  from
                      transfer_tasks_child ttc left join transfer_worker tw on ttc.assigned_to = tw."uuid"
                  where
                      assigned_to is not null and tw."uuid" is null
              ) AND
                  transfer_tasks_child.status != ANY(?);
            """ ;
    public static final String RESTART_UNASSIGNED_BUT_IN_PROGRESS_TASKS =
            """
              update
                  transfer_tasks_child
              set
                  status = 'ACCEPTED'
              where
                  status = 'IN_PROGRESS' AND
                  assigned_to IS NULL;
            """ ;

    public static final String INSERT_CHILD_TASK =
            "INSERT into transfer_tasks_child " +
                    " (tenant_id, task_id, parent_task_id, username, source_uri, destination_uri, status, bytes_transferred, total_bytes, is_dir, is_executable, tag, external_task_id)" +
                    " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                    " RETURNING * ";
}
