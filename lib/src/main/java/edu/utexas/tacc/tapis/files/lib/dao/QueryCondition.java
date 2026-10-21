package edu.utexas.tacc.tapis.files.lib.dao;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class QueryCondition {
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

//    public enum CollectionComparator {
//        IN,
//        NOT_IN,
//    }

    public static <R extends Record, T> QueryCondition from(ComparableField<R, T> field, Comparator comparator, T value) {
        return new QueryCondition(field.getJooqField().compare(getJooqComparator(comparator), value));
    };

    public static <R extends Record, T> QueryCondition isNull(ComparableField<R, T> field) {
        return new QueryCondition(field.getJooqField().isNull());
    }

    public static <R extends Record, T> QueryCondition isNotNull(ComparableField<R, T> field) {
        return new QueryCondition(field.getJooqField().isNotNull());
    }

    public static <R extends Record, T> QueryCondition isIn(ComparableField<R, T> field, Collection<? extends T> values) {
        Field<T> jooqField = field.getJooqField();
        return new QueryCondition(jooqField.in(values));
    }

    public static <R extends Record, T> QueryCondition notIn(ComparableField<R, T> field, Collection<? extends T> values) {
        Field<T> jooqField = field.getJooqField();
        return new QueryCondition(jooqField.notIn(values));
    }

    public static QueryCondition and(QueryCondition ... queryConditions ) {
        if (queryConditions == null || queryConditions.length == 0)
            return new QueryCondition(DSL.noCondition());

        return new QueryCondition(DSL.and(queryArrayToJooqList(queryConditions)));
    }

    public static QueryCondition or(QueryCondition ... queryConditions ) {
        if (queryConditions == null || queryConditions.length == 0)
            return new QueryCondition(DSL.noCondition());

        return new QueryCondition(DSL.or(queryArrayToJooqList(queryConditions)));
    }

    private static List<Condition> queryArrayToJooqList(QueryCondition[] queryConditions) {
        List<Condition> jooqList = new ArrayList<>();
        for (QueryCondition queryCondition : queryConditions) {
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
