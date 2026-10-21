package edu.utexas.tacc.tapis.files.lib.dao;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class QueryCondition<R extends Record> {
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

    public static <R extends Record, T> QueryCondition<R> isEqual(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.EQUALS), value));
    };

    public static <R extends Record, T> QueryCondition<R> isNotEqual(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.NOT_EQUALS), value));
    };

    public static <R extends Record, T> QueryCondition<R> isLess(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.LESS), value));
    };

    public static <R extends Record, T> QueryCondition<R> isLessOrEqual(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.LESS_OR_EQUAL), value));
    };

    public static <R extends Record, T> QueryCondition<R> isGreater(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.GREATER), value));
    };

    public static <R extends Record, T> QueryCondition<R> isGreaterOrEqual(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.GREATER_OR_EQUAL), value));
    };

    public static <R extends Record, T> QueryCondition<R> isNull(ComparableField<R, T> field) {
        return new QueryCondition<R>(field.getJooqField().isNull());
    }

    public static <R extends Record, T> QueryCondition<R> isNotNull(ComparableField<R, T> field) {
        return new QueryCondition<R>(field.getJooqField().isNotNull());
    }

    public static <R extends Record, T> QueryCondition<R> isIn(ComparableField<R, T> field, Collection<? extends T> values) {
        Field<T> jooqField = field.getJooqField();
        return new QueryCondition<R>(jooqField.in(values));
    }

    public static <R extends Record, T> QueryCondition<R> notIn(ComparableField<R, T> field, Collection<? extends T> values) {
        Field<T> jooqField = field.getJooqField();
        return new QueryCondition<R>(jooqField.notIn(values));
    }

    public static <R extends Record> QueryCondition<R> and(QueryCondition<R> ... queryConditions ) {
        if (queryConditions == null || queryConditions.length == 0)
            return new QueryCondition(DSL.noCondition());

        return new QueryCondition(DSL.and(queryArrayToJooqList(queryConditions)));
    }

    public static <R extends Record> QueryCondition<R> or(QueryCondition<R> ... queryConditions ) {
        if (queryConditions == null || queryConditions.length == 0)
            return new QueryCondition(DSL.noCondition());

        return new QueryCondition(DSL.or(queryArrayToJooqList(queryConditions)));
    }

    private static <R extends Record> List<Condition> queryArrayToJooqList(QueryCondition<R>[] queryConditions) {
        List<Condition> jooqList = new ArrayList<>();
        for (QueryCondition<R> queryCondition : queryConditions) {
            jooqList.add(queryCondition.getCondition());
        }
        return jooqList;
    }
    private Condition condition = DSL.noCondition();

    public static class ComparableField<R extends Record, T> extends FilesQueryBuilder.MappedField<R, T> {
        public ComparableField(TableField<R, T> jooqField) {
            super(jooqField);
        }
    }

    private QueryCondition(Condition condition) {
        this.condition = condition;
    }

    Condition getCondition() {
        return condition;
    }

    private static org.jooq.Comparator getJooqComparator(Comparator comparator) {
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

}
