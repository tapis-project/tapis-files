package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.util.List;

public class FilesDAOHelper {
    public <R extends Record> List<R> fetch(DAOTransactionContext context, FilesQueryBuilder<R> query) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());
        var select = db.selectFrom(query.getTable())
                .where(query.buildCondition())
                .orderBy(query.buildSortFields());
        if (query.getOffset() != null) {
            select.offset(query.getOffset());
        }
        if (query.getLimit() != null) {
            select.limit(query.getLimit());
        }
        return select.fetch();
    }

    public <R extends Record> R fetchOne(DAOTransactionContext context, FilesQueryBuilder<R> query) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());
        var select = db.selectFrom(query.getTable())
                .where(query.buildCondition())
                .orderBy(query.buildSortFields());
        if (query.getOffset() != null) {
            select.offset(query.getOffset());
        }
        if (query.getLimit() != null) {
            select.limit(query.getLimit());
        }
        return select.fetchOne();
    }

    public <R extends Record> int fetchCount(DAOTransactionContext context, FilesQueryBuilder<R> query) throws DAOException {
        DSLContext db = DSL.using(context.getConnection());
        return db.fetchCount(query.getTable(), query.buildCondition());
    }
}
