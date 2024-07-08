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
                  status = 'ACCEPTED' AND
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

    public static final String UNASSIGN_TASKS_FROM_WORKER =
            """
              update 
                  transfer_tasks_child ttc 
              set 
                  assigned_to = NULL
              where 
                  assigned_to = ?;
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
              );
            """ ;

}
