package edu.utexas.tacc.tapis.files.lib.dao.transfers;

public class TransferTaskParentDAOStatements {
    public static final String GET_ACCEPTED_PARENT_TASKS_FOR_TENANTS_AND_USERS =
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
                  transfer_tasks_parent
                where 
                  status = 'ACCEPTED' AND
                  assigned_to IS NULL
              )
                where
                  row_number <= ?
                order by
                  row_number;
                      
            """ ;

    public static final String GET_ACCEPTED_PARENT_TASKS_ASSIGNED_TO_WORKER =
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
                  transfer_tasks_parent
                where 
                  status = 'ACCEPTED' AND
                  assigned_to = ?
              )
                where
                  row_number <= ?
                order by
                  row_number;
                      
            """ ;


    public static final String GET_ASSIGNED_PARENT_COUNT =
            """
              select 
                  assigned_to, count(*) 
              from 
                  transfer_tasks_parent ttp 
                  inner join transfer_worker tw on ttp.assigned_to=tw.uuid 
              group by 
                  assigned_to;
            """ ;


    public static final String ASSIGN_TASKS_TO_WORKER =
            """
              update 
                  transfer_tasks_parent ttp
              set 
                  assigned_to = ?
              where 
                  id = ANY(?);
            """ ;

    public static final String UNASSIGN_ZOMBIE_ASSIGNMENTS =
            """
              update 
                  transfer_tasks_parent 
              set 
                  assigned_to = null 
              where id in (
                  select 
                      id 
                  from 
                      transfer_tasks_parent ttp left join transfer_worker tw on ttp.assigned_to = tw."uuid" 
                  where 
                      assigned_to is not null and tw."uuid" is null
              ) AND 
                  transfer_tasks_parent.status != ANY(?);
            """ ;

    public static final String RESET_UNASSIGNED_BUT_IN_STAGING_TASKS =
            """
              update 
                  transfer_tasks_parent 
              set 
                  status = 'ACCEPTED'
              where
                  status = 'STAGING' AND
                  assigned_to IS NULL
              returning task_id;
            """ ;
    public static final String FAIL_ASSOCIATED_TOP_TASKS =
            """
                update 
                    transfer_tasks 
                    set 
                        status = 'FAILED' 
                where id 
                in (
                    select 
                        tt.id 
                    from 
                        transfer_tasks tt 
                    inner join 
                        transfer_tasks_parent ttp 
                        on 
                            tt.id = ttp.task_id 
                    where 
                        tt.id = ANY(?) and ttp.optional
                ); 
            """ ;
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
                             final_message = ?,
                             error_message = ?,
                             assigned_to = ?
                        WHERE uuid = ? 
                        RETURNING *
            """;

}
