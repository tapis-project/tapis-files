package edu.utexas.tacc.tapis.files.lib.dao;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static edu.utexas.tacc.tapis.files.lib.dao.QueryBuilder.ComparableField;
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

    public static <R extends Record, T> QueryCondition<R> isDistinctFrom(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.IS_DISTINCT_FROM), value));
    };

    public static <R extends Record, T> QueryCondition<R> isNotDistinctFrom(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.IS_NOT_DISTINCT_FROM), value));
    };

    public static <R extends Record, T> QueryCondition<R> isLike(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.LIKE), value));
    };

    public static <R extends Record, T> QueryCondition<R> isNotLike(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.NOT_LIKE), value));
    };

    public static <R extends Record, T> QueryCondition<R> isLikeIgnoreCase(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.LIKE_IGNORE_CASE), value));
    };

    public static <R extends Record, T> QueryCondition<R> isNotLikeIgnoreCase(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.NOT_LIKE_IGNORE_CASE), value));
    };

    public static <R extends Record, T> QueryCondition<R> isSimilarTo(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.SIMILAR_TO), value));
    };

    public static <R extends Record, T> QueryCondition<R> isNotSimilarTo(ComparableField<R, T> field, T value) {
        return new QueryCondition<R>(field.getJooqField().compare(getJooqComparator(Comparator.NOT_SIMILAR_TO), value));
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
