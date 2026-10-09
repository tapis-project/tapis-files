package edu.utexas.tacc.tapis.files.lib.dao.stats;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.SshSessionLogRecord;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import edu.utexas.tacc.tapis.files.lib.models.TransferTaskStatus;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ConnectionDetails;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.StatusSummary;
import edu.utexas.tacc.tapis.shared.ssh.stats.SessionPoolDetails;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import org.jooq.CommonTableExpression;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksParent.TRANSFER_TASKS_PARENT;
import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasksChild.TRANSFER_TASKS_CHILD;
import static edu.utexas.tacc.tapis.files.gen.jooq.tables.TransferTasks.TRANSFER_TASKS;
import static edu.utexas.tacc.tapis.files.gen.jooq.tables.SshSessionLog.SSH_SESSION_LOG;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.rowNumber;
import static org.jooq.impl.DSL.partitionBy;

public class ManagementStatsDAO {
    private static final Logger log = LoggerFactory.getLogger(ManagementStatsDAO.class);
    public enum AssignmentStatus {
        ASSIGNED,
        UNASSIGNED,
        ALL
    }

    // TODO:  rename unnassigned to queued_unassigned or something like that.  Make other names match too.
    public StatusSummary getSummary(DAOTransactionContext context) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());

        StatusSummary statusSummary = new StatusSummary();
        Select<Record> queuedTopTaskBaseQuery = getTopTasksInStatusBaseQuery(db, List.of( TransferTaskStatus.ACCEPTED), Optional.empty());
        statusSummary.setQueuedTopTasks(db.fetchCount(queuedTopTaskBaseQuery));
        Select<Record> inProgressTopTaskBaseQuery = getTopTasksInStatusBaseQuery(db, List.of( TransferTaskStatus.IN_PROGRESS), Optional.empty());
        statusSummary.setInProgressTopTasks(db.fetchCount(inProgressTopTaskBaseQuery));
        Select<Record> inProgressParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db, Optional.of(List.of( TransferTaskStatus.IN_PROGRESS )), AssignmentStatus.ALL);
        statusSummary.setInProgressParentTasks(db.fetchCount(inProgressParentTaskBaseQuery));
        Select<Record> inProgressChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db, Optional.of(List.of( TransferTaskStatus.IN_PROGRESS )), AssignmentStatus.ALL);
        statusSummary.setInProgressChildTasks(db.fetchCount(inProgressChildTaskBaseQuery));

        Select<Record> assignedParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db,
                Optional.empty(),
                AssignmentStatus.ASSIGNED);
        statusSummary.setAssignedParentTasks(db.fetchCount(assignedParentTaskBaseQuery));
        Select<Record> assignedChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db,
                Optional.empty(),
                AssignmentStatus.ASSIGNED);
        statusSummary.setAssignedChildTasks(db.fetchCount(assignedChildTaskBaseQuery));

        Select<Record> unassignedParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db,
                Optional.of(List.of(TransferTaskStatus.ACCEPTED)),
                AssignmentStatus.UNASSIGNED);
        statusSummary.setUnassignedParentTasks(db.fetchCount(unassignedParentTaskBaseQuery));
        Select<Record> unassignedChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db,
                Optional.of(List.of(TransferTaskStatus.ACCEPTED)),
                AssignmentStatus.UNASSIGNED);
        statusSummary.setUnassignedChildTasks(db.fetchCount(unassignedChildTaskBaseQuery));

        Select<Record> awaitingRetryParentTaskBaseQuery = getParentTasksInStatusBaseQuery(db,
                Optional.of( List.of(TransferTaskStatus.AWAITING_RETRY) ),
                AssignmentStatus.ALL);
        statusSummary.setAwaitingRetryParentTasks(db.fetchCount(awaitingRetryParentTaskBaseQuery));
        Select<Record> awaitingRetryChildTaskBaseQuery = getChildTasksInStatusBaseQuery(db,
                Optional.of( List.of(TransferTaskStatus.AWAITING_RETRY) ),
                AssignmentStatus.ALL);
        statusSummary.setAwaitingRetryChildTasks(db.fetchCount(awaitingRetryChildTaskBaseQuery));

        return statusSummary;
    }

    // top queries
    private Select<Record> getTopTasksInStatusBaseQuery(DSLContext db, List<TransferTaskStatus> statuses, Optional<Integer> limit) {
        var query = db.select().from(TRANSFER_TASKS).
                where(TRANSFER_TASKS.STATUS.in(statuses));
        if(limit.isPresent()) {
            return query.limit(limit.get());
        } else {
            return query;
        }
    }

    // parent queries
    private Select<Record> getParentTasksInStatusBaseQuery(DSLContext db, Optional<List<TransferTaskStatus>> statuses, AssignmentStatus assignmentStatus) throws DAOException {
        return db.select().from(TRANSFER_TASKS_PARENT).where(
                statuses.isPresent() ? TRANSFER_TASKS_PARENT.STATUS.in(statuses.get()) : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.ASSIGNED) ? TRANSFER_TASKS_PARENT.ASSIGNED_TO.isNotNull() : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.UNASSIGNED) ? TRANSFER_TASKS_PARENT.ASSIGNED_TO.isNull() : DSL.noCondition()
        );
    }

    // child queries
    private Select<Record> getChildTasksInStatusBaseQuery(DSLContext db, Optional<List<TransferTaskStatus>> statuses, AssignmentStatus assignmentStatus) throws DAOException {
        return db.select().from(TRANSFER_TASKS_CHILD).where(
                statuses.isPresent() ? TRANSFER_TASKS_CHILD.STATUS.in(statuses.get()) : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.ASSIGNED) ? TRANSFER_TASKS_CHILD.ASSIGNED_TO.isNotNull() : DSL.noCondition(),
                assignmentStatus.equals(AssignmentStatus.UNASSIGNED) ? TRANSFER_TASKS_CHILD.ASSIGNED_TO.isNull() : DSL.noCondition()
        );
    }

    // Ssh status queries
    public int saveSshStats(DAOTransactionContext context, ConnectionDetails details) {
        DSLContext db = DSL.using(context.getConnection());
        Record insertedRecord = db.insertInto(SSH_SESSION_LOG)
                .set(SSH_SESSION_LOG.SESSION_STATS, JSONB.jsonbOrNull(TapisGsonUtils.getGson().toJson(details.getSessionPoolDetails())))
                .set(SSH_SESSION_LOG.CONTEXT_NAME, details.getContextName())
                .returningResult(SSH_SESSION_LOG.asterisk())
                .fetchOne();
        SshSessionLogRecord sshSessionLogRecord = SSH_SESSION_LOG.from(insertedRecord);
        return 0;
    }

    public List<ConnectionDetails> getSshStats(DAOTransactionContext context) {
        DSLContext db = DSL.using(context.getConnection());

        Field<Integer> rowNumberField = rowNumber()
                .over(
                        partitionBy(SSH_SESSION_LOG.CONTEXT_NAME)
                                .orderBy(SSH_SESSION_LOG.CREATED.desc())
                )
                .as("row_number");

        CommonTableExpression<Record> sessions = name("sessions").as(
                select(SSH_SESSION_LOG.asterisk(), rowNumberField)
                        .from(SSH_SESSION_LOG).orderBy(SSH_SESSION_LOG.CREATED.desc())
        );

        return db.with(sessions)
                .selectFrom(sessions)
                .where(sessions.field("row_number", Integer.class).eq(1))
                .fetch().stream().map(record -> {
                    SshSessionLogRecord logRecord = SSH_SESSION_LOG.from(record);
                    ConnectionDetails connectionDetails = new ConnectionDetails();
                    connectionDetails.setContextName(logRecord.getContextName());
                    String jsonString = logRecord.getSessionStats().toString();
                    SessionPoolDetails details = TapisGsonUtils.getGson().fromJson(jsonString,  SessionPoolDetails.class);
                    connectionDetails.setSessionPoolDetails(details);
                    connectionDetails.setUpdated(instantOfLocalDateTime(logRecord.getUpdated()));
                    connectionDetails.setCreated(instantOfLocalDateTime(logRecord.getCreated()));
                    return connectionDetails;
                }).toList();
    }

    public int deleteOlderThan(DAOTransactionContext context, String contextName, Instant time) {
        DSLContext db = DSL.using(context.getConnection());
        return db.delete(SSH_SESSION_LOG)
                .where(SSH_SESSION_LOG.UPDATED.lessThan(LocalDateTime.ofInstant(time, ZoneOffset.UTC)))
                .execute();
    }
    private Instant instantOfLocalDateTime(LocalDateTime ldt) {
        if(ldt == null) {
            return null;
        }

        return ldt.toInstant(ZoneOffset.UTC);
    }


}

