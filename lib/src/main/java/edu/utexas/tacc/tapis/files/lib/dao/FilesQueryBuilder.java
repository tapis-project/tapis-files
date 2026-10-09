package edu.utexas.tacc.tapis.files.lib.dao;

import edu.utexas.tacc.tapis.files.lib.exceptions.DAOException;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SortField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class FilesQueryBuilder<R extends Record> {
    public enum SortOrder {
        ASC,
        DESC,
        DEFAULT
    }

    public enum Comparator {
        EQUALS,
        NOT_EQUALS,
        LESS,
        LESS_OR_EQUAL,
        GREATER,
        GREATER_OR_EQUAL,
        IS_DISTINCT_FROM,
        IS_NOT_DISTINCT_FROM,
        LIKE,
        NOT_LIKE,
        SIMILAR_TO,
        NOT_SIMILAR_TO,
        LIKE_IGNORE_CASE,
        NOT_LIKE_IGNORE_CASE,
    }

    public enum CollectionComparator {
        IN,
        NOT_IN,
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

    public static class ComparableField<R extends Record, T> extends MappedField<R, T>{
        public ComparableField(TableField<R, T> jooqField) {
            super(jooqField);
        }
    }

    private final Table<R> table;
    private List<SortField<?>> sortFields = new ArrayList<>();
    List<Condition> conditions = new ArrayList<>();
    Integer offset;
    Integer limit;

    FilesQueryBuilder(Table<R> table) {
        this.table = table;
    }

    public   <T> boolean addCollectionConditon(ComparableField<R, T> field, CollectionComparator comparator, Collection<? extends T> values) {
        Field<T> jooqField = field.getJooqField();
        return conditions.add(
                switch (comparator) {
                    case IN -> jooqField.in(values);
                    case NOT_IN -> jooqField.in(values);
                });
    }

    public <T> boolean addNullCondition(ComparableField<R, T> field) {
        return conditions.add(field.getJooqField().isNull());
    }

    public <T> boolean addNotNullCondition(ComparableField<R, T> field) {
        return conditions.add(field.getJooqField().isNotNull());
    }

    public <T> boolean addCondition(ComparableField<R, T> field, Comparator comparator, T value) {
        return conditions.add(field.getJooqField().compare(getJooqComparator(comparator), value));
    }

    public <T> boolean addSortField(SortableField<R, T> field, SortOrder sortOrder) {
        return sortFields.add(field.getJooqField().sort(getJooqSortOrder(sortOrder)));
    }

    // TODO: These are all AND's ... do we need or?  is there a not? etc.
    protected Condition buildCondition() {
        Condition returnCondition = DSL.noCondition();

        for (Condition condition : conditions) {
            returnCondition = returnCondition.and(condition);
        }

        return returnCondition;
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

    private org.jooq.Comparator getJooqComparator(CollectionComparator comparator) {
        return switch (comparator) {
            case IN -> org.jooq.Comparator.IN;
            case NOT_IN -> org.jooq.Comparator.NOT_IN;
        };
    }

    private org.jooq.Comparator getJooqComparator(Comparator comparator) {
        return switch (comparator) {
            case EQUALS -> org.jooq.Comparator.EQUALS;
            case NOT_EQUALS -> org.jooq.Comparator.NOT_EQUALS;
            case LESS -> org.jooq.Comparator.LESS;
            case LESS_OR_EQUAL -> org.jooq.Comparator.LESS_OR_EQUAL;
            case GREATER -> org.jooq.Comparator.GREATER;
            case GREATER_OR_EQUAL -> org.jooq.Comparator.GREATER_OR_EQUAL;
            case IS_DISTINCT_FROM -> org.jooq.Comparator.IS_DISTINCT_FROM;
            case IS_NOT_DISTINCT_FROM -> org.jooq.Comparator.IS_NOT_DISTINCT_FROM;
            case LIKE -> org.jooq.Comparator.LIKE;
            case NOT_LIKE -> org.jooq.Comparator.NOT_LIKE;
            case SIMILAR_TO -> org.jooq.Comparator.SIMILAR_TO;
            case NOT_SIMILAR_TO -> org.jooq.Comparator.NOT_SIMILAR_TO;
            case LIKE_IGNORE_CASE -> org.jooq.Comparator.LIKE_IGNORE_CASE;
            case NOT_LIKE_IGNORE_CASE -> org.jooq.Comparator.NOT_LIKE_IGNORE_CASE;
        };
    }

    private org.jooq.SortOrder getJooqSortOrder(SortOrder sortOrder) {
        return switch (sortOrder) {
            case ASC -> org.jooq.SortOrder.ASC;
            case DESC -> org.jooq.SortOrder.DESC;
            case DEFAULT -> org.jooq.SortOrder.DEFAULT;
        };
    }
}
