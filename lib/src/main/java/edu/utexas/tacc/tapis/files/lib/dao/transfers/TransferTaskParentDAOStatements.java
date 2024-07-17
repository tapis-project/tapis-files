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

}
