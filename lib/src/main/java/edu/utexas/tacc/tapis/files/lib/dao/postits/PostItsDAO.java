package edu.utexas.tacc.tapis.files.lib.dao.postits;

import static edu.utexas.tacc.tapis.files.gen.jooq.tables.FilesPostits.FILES_POSTITS;

import edu.utexas.tacc.tapis.files.gen.jooq.tables.records.FilesPostitsRecord;
import edu.utexas.tacc.tapis.files.lib.database.HikariConnectionPool;
import edu.utexas.tacc.tapis.files.lib.models.PostIt;
import edu.utexas.tacc.tapis.files.lib.utils.LibUtils;
import edu.utexas.tacc.tapis.search.SearchUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisDBConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.OrderBy;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;
import edu.utexas.tacc.tapis.shareddb.datasource.TapisDataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SortField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PostItsDAO {

    private static final Logger log = LoggerFactory.getLogger(PostItsDAO.class);

    /**
     * Create a new PostIt record in the database.  The following
     * fields are ignored and are auto-populated by this method:
     * <p/>
     * Id
     * Created
     * Updated
     * <p/>
     *
     * @param postIt
     *
     * @return the newly created PostIt.
     * @throws TapisException
     */
    public PostIt createPostIt(PostIt postIt) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);

            // Insert the PostIt in the database.
            LocalDateTime now = localDateTimeOfInstant(Instant.now());
            Record insertRecord = db.insertInto(FILES_POSTITS).
                                    set(FILES_POSTITS.ID, new TapisUUID(UUIDType.POSTIT).toString()).
                                    set(FILES_POSTITS.SYSTEM_ID, postIt.getSystemId()).
                                    set(FILES_POSTITS.PATH, postIt.getPath()).
                                    set(FILES_POSTITS.ALLOWED_USES, postIt.getAllowedUses()).
                                    set(FILES_POSTITS.TIMES_USED, postIt.getTimesUsed()).
                                    set(FILES_POSTITS.JWT_USER, postIt.getJwtUser()).
                                    set(FILES_POSTITS.JWT_TENANT_ID, postIt.getJwtTenantId()).
                                    set(FILES_POSTITS.OWNER, postIt.getOwner()).
                                    set(FILES_POSTITS.TENANT_ID, postIt.getTenantId()).
                                    set(FILES_POSTITS.EXPIRATION, localDateTimeOfInstant(postIt.getExpiration())).
                                    set(FILES_POSTITS.CREATED, now).
                                    set(FILES_POSTITS.UPDATED, now).
                                    returningResult(FILES_POSTITS.asterisk()).
                                    fetchOne();
            if(insertRecord == null) {
                // Record must not have been inserted for some reason.  Normally this
                // would result in an exception, but just for completeness, handle this
                // case.
                String msg = LibUtils.getMsg("POSTITS_DAO_ERROR_ID", postIt.getId(), "No record returned on insert");
                log.error(msg);
                throw new TapisException(msg);
            }

            postItRecord = FILES_POSTITS.from(insertRecord);
            logRecord("Inserted Record:", postItRecord);
            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID", postIt.getId(), e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return createPostItFromRecord(postItRecord);
    }

    /**
     * Get a single PostIt by Id.
     *
     * @param postItId id of the PostIt to retrieve.
     * @return the PostIt record specified by the Id, or null if a PostIt
     * with the specified Id does not exist.
     * @throws TapisException
     */
    public PostIt getPostIt(String postItId) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);
            Record selectRecord = db.select().from(FILES_POSTITS).
                    where(FILES_POSTITS.ID.eq(postItId)).
                    fetchOne();

            if(selectRecord != null) {
                postItRecord = FILES_POSTITS.from(selectRecord);
                logRecord("Retrieved Record:", postItRecord);
            }

            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID",
                    postItId, e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return createPostItFromRecord(postItRecord);
    }

    /**
     * Returns a list of PostIts based on the condition passed in.
     *
     * @param tenantId The Id of the tenant to list PostIts for
     * @param ownerId The Id of the owner to list PostIts for - if no owner is specified,
     *               all PostIts for the tenant will be returned.
     * @return returns a non-null (but possibly empty) list of PostIts matching
     * the criteria in the condition.
     * @throws TapisException
     */
    public List<PostIt> listPostIts(String tenantId, String ownerId, Integer limit,
                                    List<OrderBy> orderByList, Integer skip,
                                    String startAfter) throws TapisException {
        List<PostIt> postIts = new ArrayList<PostIt>();
        Connection conn = null;

        if((skip > 0) && (!StringUtils.isBlank(startAfter))) {
            String msg = LibUtils.getMsg("POSTITS_DAO_SKIP_STARTAFTER", skip, startAfter);
            log.info(msg);
            throw new TapisException(msg);
        }

        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);

            Condition whereCondition = getWhereCondition(tenantId, ownerId);
            List<SortField<?>> orderFields = getOrderFields(orderByList);

            Condition startAfterCondition = getStartAfterCondition(orderByList, startAfter);
            if(startAfterCondition != null) {
                whereCondition = whereCondition.and(startAfterCondition);
            }

            List<Record> selectRecords =
                    db.select().from(FILES_POSTITS).
                            where(whereCondition).
                            orderBy(orderFields).
                            offset(skip).
                            limit(limit).
                            fetch();
            postIts = selectRecords.stream().map( record -> {
                FilesPostitsRecord postItRecord = FILES_POSTITS.from(record);
                return createPostItFromRecord(postItRecord);
            }).collect(Collectors.toList());
            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_LIST_ERROR", e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return postIts;
    }

    private Condition getWhereCondition(String tenantId, String ownerId) {
        Condition whereCondition = FILES_POSTITS.TENANT_ID.equal(tenantId);

        if(!StringUtils.isBlank(ownerId)) {
            whereCondition = whereCondition.and(FILES_POSTITS.OWNER.equal(ownerId));
        }

        return whereCondition;
    }

    private List<SortField<?>> getOrderFields(List<OrderBy> orderByList) throws TapisException {
        List<SortField<?>> orderFields = new ArrayList<>();

        for(OrderBy orderBy : orderByList) {
            Pair<Field, SortOrder> fieldOrder = getFieldOrder(orderBy);
            orderFields.add(fieldOrder.getLeft().sort(fieldOrder.getRight()));
        }

        return orderFields;
    }

    private Pair<Field, SortOrder> getFieldOrder(OrderBy orderBy)
            throws TapisException {
        SortOrder sortOrder = null;
        switch(orderBy.getOrderByDir()) {
            case ASC -> sortOrder = SortOrder.ASC;
            case DESC -> sortOrder = SortOrder.DESC;
            default -> sortOrder = SortOrder.DEFAULT;
        }

        String orderByAttr = orderBy.getOrderByAttr();
        Field orderField = getField(orderByAttr);

        return Pair.of(orderField, sortOrder);
    }

    private Field getField(String fieldName) throws TapisException {
        Field field = FILES_POSTITS.field(DSL.name(SearchUtils.camelCaseToSnakeCase(fieldName)));

        if(field == null) {
            String msg = LibUtils.getMsg("POSTITS_SERVICE_FIELD_NOT_FOUND", fieldName);
            log.error(msg);
            throw new TapisException(msg);
        }

        return field;
    }

    private Condition getStartAfterCondition(List<OrderBy> orderByList, String startAfter)
            throws TapisException {
        Pair<Field, SortOrder> primaryKey = null;

        if (!StringUtils.isBlank(startAfter)) {
            if(!CollectionUtils.isEmpty(orderByList)) {
                primaryKey = getFieldOrder(orderByList.get(0));
            }
        } else {
            // no startAfter supplied
            return null;
        }

        if(primaryKey == null) {
            String msg = LibUtils.getMsg("POSTITS_DAO_STARTAFTER_REQUIRES_SORT");
            log.info(msg);
            throw new TapisException(msg);
        }

        Condition startAfterCondition = null;
        Field field = primaryKey.getLeft();
        SortOrder sortOrder = primaryKey.getRight();
        if(sortOrder.equals(SortOrder.ASC)) {
            startAfterCondition = field.greaterThan(startAfter);
        } else if (sortOrder.equals(SortOrder.DESC)) {
            startAfterCondition = field.lessThan(startAfter);
        } else {
            String msg = LibUtils.getMsg("POSTITS_DAO_STARTAFTER_REQUIRES_SORT");
            log.info(msg);
            throw new TapisException(msg);
        }

        return startAfterCondition;
    }

    /**
     * Update's a PostIt.  The only fields that can be updated for a PostIt are:
     *
     * allowedUses
     * expiration
     *
     * All other fields will be ignored
     *
     * @param updatePostIt A PostIt containing the updated fields.  The ID of the
     *               PostIt must be set.
     *
     * @return the newly updated PostIt
     */
    public PostIt updatePostIt(PostIt updatePostIt) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;

        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);
            PostIt postIt = prepareUpdate(db, updatePostIt);
            Record updatedRecord = db.update(FILES_POSTITS).
                    set(FILES_POSTITS.ALLOWED_USES, postIt.getAllowedUses()).
                    set(FILES_POSTITS.EXPIRATION, localDateTimeOfInstant(postIt.getExpiration())).
                    set(FILES_POSTITS.UPDATED, localDateTimeOfInstant(Instant.now())).
                    where(FILES_POSTITS.ID.equal(postIt.getId())).
                    returningResult().
                    fetchOne();

            if (updatedRecord != null) {
                postItRecord = FILES_POSTITS.from(updatedRecord);
                logRecord("Updated Record:", postItRecord);
            }

            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID",
                    updatePostIt.getId(), e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return createPostItFromRecord(postItRecord);
    }

    /**
     * Increments the use count for a PostIt.  The PostIt will be read (locked), and
     * checked for expiration (both by time and by usage count).  If the postIt is
     * redeemable now, the count will be incremented, and the postIt will be returned.
     * If the PostIt is expired or is not found, the method will return null.
     *
     * @param postItId
     * @return true if the PostItUseCount was updated, or false if it was not redeemable.
     * @throws TapisException
     */
    public boolean incrementUseIfRedeemable(String postItId) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord updatedPostItRecord = null;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);

            PostIt currentPostIt = getPostItForUpdate(db, postItId);
            if(!currentPostIt.isRedeemable()) {
                return false;
            }
            if (currentPostIt.isRedeemable()) {
                Record updateRecord = db.update(FILES_POSTITS).
                        set(FILES_POSTITS.TIMES_USED, FILES_POSTITS.TIMES_USED.plus(1)).
                        set(FILES_POSTITS.UPDATED, localDateTimeOfInstant(Instant.now())).
                        where(FILES_POSTITS.ID.equal(postItId)).
                        returningResult(FILES_POSTITS.asterisk()).
                        fetchOne();

                if (updateRecord == null) {
                    // We should be able to update this record, so if something
                    // happens and we get nothing back, something bad has happened.
                    String msg = LibUtils.getMsg("POSTIT_NOT_FOUND", postItId);
                    log.error(msg);
                    throw new TapisException(msg);
                }

                updatedPostItRecord = FILES_POSTITS.from(updateRecord);
                logRecord("Updated Record:", updatedPostItRecord);
            } else {
                String msg = LibUtils.getMsg("POSTIT_NOT_REDEEMABLE", postItId);
                log.warn(msg);
                throw new TapisException(msg);
            }
            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_ERROR_ID", postItId, e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return true;
    }

    /**
     * Delete a PostIt by Id.
     * @param postItId Id of the postit to delete.
     * @return count of deleted objects (should be 1 assuming everything went well).
     * @throws TapisException
     */
    public int deletePostIt(String postItId) throws TapisException {
        return deletePostIt(FILES_POSTITS.ID.eq(postItId));
    }

    /**
     * Delete all expired PostIts.  A PostIt is expired if it is past the expiration
     * date, or if it has been redeemed at the number of times allowed or more.  It
     * could happen that a postit is updated to have fewer uses than have already been
     * redeemed, so it's important to note that we are checking for uses equal or greater
     * than the allowed uses.
     * @return count of deleted objects
     * @throws TapisException
     */
    public int deleteExpiredPostIts() throws TapisException {
        return deletePostIt(FILES_POSTITS.EXPIRATION.lessThan(localDateTimeOfInstant(Instant.now())).
                or(FILES_POSTITS.TIMES_USED.greaterOrEqual(FILES_POSTITS.ALLOWED_USES)));
    }

    private int deletePostIt(Condition deleteCondition) throws TapisException {
        Connection conn = null;
        FilesPostitsRecord postItRecord = null;
        int deleteCount = 0;
        try {
            conn = getConnection();
            DSLContext db = DSL.using(conn);
            deleteCount = db.delete(FILES_POSTITS).
                    where(deleteCondition).
                    execute();

            LibUtils.closeAndCommitDB(conn, null, null);
        } catch (Exception e) {
            // Rollback transaction and throw an exception
            LibUtils.rollbackDB(conn, e,"POSTITS_DAO_DELETE_ERROR",
                    deleteCondition, e.getMessage());
        } finally {
            // Always return the connection back to the connection pool.
            LibUtils.finalCloseDB(conn);
        }

        return deleteCount;
    }

    // Only support updating allowedUses, expiration, and usage
    private PostIt getPostItForUpdate(DSLContext db, String postItId)
            throws TapisException {
        Record record = db.select().from(FILES_POSTITS).
                where(FILES_POSTITS.ID.equal(postItId)).
                forUpdate().
                fetchOne();

        if(record == null) {
            // the postit that we are attempting to increment the usage count for
            // does not exist.
            String msg = LibUtils.getMsg("POSTIT_NOT_FOUND", postItId);
            log.error(msg);
            throw new TapisException(msg);
        }

        FilesPostitsRecord postitsRecord = FILES_POSTITS.from(record);
        PostIt postIt = createPostItFromRecord(postitsRecord);

        return postIt;
    }

    // Only support updating allowedUses and expiration
    // NOTE - this reads FOR UPDATE (meaning it's locked, and
    // the current transaction must be committed or rolled back
    // by the caller.
    private PostIt prepareUpdate(DSLContext db, PostIt updatePostIt)
            throws TapisException {
        PostIt postIt = getPostItForUpdate(db, updatePostIt.getId());
        if(updatePostIt.getAllowedUses() != null) {
            postIt.setAllowedUses(updatePostIt.getAllowedUses());
        }
        if(updatePostIt.getExpiration() != null) {
            postIt.setExpiration(updatePostIt.getExpiration());
        }
        return postIt;
    }

    private static synchronized Connection getConnection()
            throws TapisException
    {
        // Use the existing datasource.
        DataSource ds = getDataSource();

        // Get the connection.
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION");
            log.error(msg, e);
            throw new TapisDBConnectionException(msg, e);
        }

        return conn;
    }

    private static DataSource getDataSource() throws TapisException
    {
        // Use the existing datasource.
        DataSource ds = TapisDataSource.getDataSource();
        if (ds == null) {
            // Get a database connection.
            ds = HikariConnectionPool.getDataSource();
        }

        return ds;
    }


    // Creates a PostIt that can be returned from the DAO given a record read from the database
    private PostIt createPostItFromRecord(FilesPostitsRecord record) {
        if(record == null) {
            return null;
        }

        PostIt postIt = new PostIt();
        postIt.setId(record.getId());
        postIt.setSystemId(record.getSystemId());
        postIt.setPath(record.getPath());
        postIt.setExpiration(instantOfLocalDateTime(record.getExpiration()));
        postIt.setAllowedUses(record.getAllowedUses());
        postIt.setOwner(record.getOwner());
        postIt.setTenantId(record.getTenantId());
        postIt.setJwtUser(record.getJwtUser());
        postIt.setJwtTenantId(record.getJwtTenantId());
        postIt.setTimesUsed(record.getTimesUsed());
        postIt.setCreated(instantOfLocalDateTime(record.getCreated()));
        postIt.setUpdated(instantOfLocalDateTime(record.getUpdated()));
        return postIt;
    }

    // Trace logging for a db record
    private void logRecord(String msg, FilesPostitsRecord record) {
        log.trace(msg);
        log.trace(record == null ? null : record.toString());
    }

    private Instant instantOfLocalDateTime(LocalDateTime ldt) {
        if(ldt == null) {
            return null;
        }

        return ldt.toInstant(ZoneOffset.UTC);
    }

    private LocalDateTime localDateTimeOfInstant(Instant instant) {
        if(instant == null) {
            return null;
        }

        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
