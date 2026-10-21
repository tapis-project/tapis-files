package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryBuilder<R extends Record> {
    public enum SortOrder {
        ASC,
        DESC,
        DEFAULT
    }

    protected static class MappedField<R extends Record, T> {
        TableField<R, T> jooqField;

        public MappedField(TableField<R, T> jooqField) {
            this.jooqField = jooqField;
        }

        public TableField<R, T> getJooqField() {
            return jooqField;
        }
    };

    public static class SortableField<R extends Record, T> extends MappedField<R, T>{
        public SortableField(TableField<R, T> jooqField) {
            super(jooqField);
        }
    }

    public static class ComparableField<R extends Record, T> extends QueryBuilder.MappedField<R, T> {
        public ComparableField(TableField<R, T> jooqField) {
            super(jooqField);
        }
    }

    private final Table<R> table;
    private List<SortField<?>> sortFields = new ArrayList<>();
    Condition jooqCondition = DSL.noCondition();
    Integer offset;
    Integer limit;

    QueryBuilder(Table<R> table) {
        this.table = table;
    }

    Condition getCondition() {
        return jooqCondition;
    }

    public void setQueryCondition(QueryCondition<R> queryCondition) {
        this.jooqCondition = queryCondition.getCondition();
    }

    public <T> boolean addSortField(SortableField<R, T> field, SortOrder sortOrder) {
        return sortFields.add(field.getJooqField().sort(getJooqSortOrder(sortOrder)));
    }

    protected List<SortField<?>> buildSortFields() throws DAOException {
        return sortFields;
    }

    protected Table<R> getTable() {
        return table;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getOffset() {
        return offset;
    }


    private org.jooq.SortOrder getJooqSortOrder(SortOrder sortOrder) {
        return switch (sortOrder) {
            case ASC -> org.jooq.SortOrder.ASC;
            case DESC -> org.jooq.SortOrder.DESC;
            case DEFAULT -> org.jooq.SortOrder.DEFAULT;
        };
    }
}
