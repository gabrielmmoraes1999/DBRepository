package io.github.gabrielmmoraes1999.db.parse;

import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlRenderer {

    private static final String ROOT_ALIAS = "p1";

    public static <T> String toSql(ParsedQuery parsedQuery, Object[] args, Class<T> entityClass) {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table");
        }

        Table table = entityClass.getAnnotation(Table.class);
        QueryType type = parsedQuery != null ? parsedQuery.type : QueryType.SELECT;

        if (type == QueryType.COUNT) {
            return buildAggregateSql("SELECT COUNT(*)", table.name(), parsedQuery, args);
        }

        if (type == QueryType.EXISTS) {
            return buildAggregateSql("SELECT COUNT(*)", table.name(), parsedQuery, args);
        }

        if (type == QueryType.DELETE) {
            return buildDeleteSql(table.name(), parsedQuery, args);
        }

        return buildSelectSql(parsedQuery, args, entityClass, table);
    }

    private static <T> String buildSelectSql(ParsedQuery parsedQuery, Object[] args, Class<T> entityClass, Table table) {
        StringBuilder sql = new StringBuilder();
        StringBuilder joins = new StringBuilder();
        StringJoiner columns = new StringJoiner(", ");
        int aliasCounter = 2;

        renderColumns(entityClass, ROOT_ALIAS, columns);

        for (Field field : entityClass.getDeclaredFields()) {
            Class<?> targetEntity;

            if (field.isAnnotationPresent(OneToOne.class)) {
                targetEntity = field.getType();
            } else if (field.isAnnotationPresent(OneToMany.class)) {
                targetEntity = getGenericType(field);
            } else {
                continue;
            }

            if (targetEntity == null || !targetEntity.isAnnotationPresent(Table.class)) {
                continue;
            }

            String joinAlias = "p" + aliasCounter++;
            Table joinTable = targetEntity.getAnnotation(Table.class);

            joins.append(" LEFT JOIN ")
                    .append(joinTable.name())
                    .append(" ")
                    .append(joinAlias)
                    .append(" ON ");

            JoinColumns joinColumns = field.getAnnotation(JoinColumns.class);
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
            String onClause = null;

            if (joinColumns != null) {
                onClause = Arrays.stream(joinColumns.value())
                        .map(jc ->
                                ROOT_ALIAS + "." + jc.referencedColumnName() +
                                        " = " +
                                        joinAlias + "." + jc.name()
                        )
                        .collect(Collectors.joining(" AND "));
            } else if (joinColumn != null) {
                onClause = Arrays.asList(joinColumn).stream().map(jc ->
                                ROOT_ALIAS + "." + jc.referencedColumnName() +
                                        " = " +
                                        joinAlias + "." + jc.name()
                        )
                        .collect(Collectors.joining(" AND "));
            }

            joins.append(onClause);
            renderColumns(targetEntity, joinAlias, columns);
        }

        sql.append("SELECT ")
                .append(columns)
                .append(" FROM ")
                .append(table.name())
                .append(" ")
                .append(ROOT_ALIAS)
                .append(joins);

        appendWhereAndOrder(sql, parsedQuery, args, ROOT_ALIAS, true);
        return sql.toString();
    }

    private static String buildAggregateSql(String selectPrefix, String tableName, ParsedQuery parsedQuery, Object[] args) {
        StringBuilder sql = new StringBuilder();
        sql.append(selectPrefix)
                .append(" FROM ")
                .append(tableName)
                .append(" ")
                .append(ROOT_ALIAS);
        appendWhereAndOrder(sql, parsedQuery, args, ROOT_ALIAS, false);
        return sql.toString();
    }

    private static String buildDeleteSql(String tableName, ParsedQuery parsedQuery, Object[] args) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName);
        appendWhereAndOrder(sql, parsedQuery, args, null, false);
        return sql.toString();
    }

    private static void appendWhereAndOrder(StringBuilder sql, ParsedQuery parsedQuery, Object[] args, String alias, boolean includeOrderBy) {
        if (parsedQuery == null) {
            return;
        }

        int[] argIndex = {0};

        if (!parsedQuery.orGroups.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(
                    parsedQuery.orGroups.stream()
                            .map(group ->
                                    group.stream()
                                            .map(c -> conditionSql(c, alias, args, argIndex))
                                            .collect(Collectors.joining(" AND "))
                            )
                            .collect(Collectors.joining(" OR "))
            );
        }

        if (includeOrderBy && parsedQuery.type == QueryType.SELECT && !parsedQuery.orderByList.isEmpty()) {
            String orderAlias = alias != null ? alias : "";
            String prefix = orderAlias.isEmpty() ? "" : orderAlias + ".";
            sql.append(" ORDER BY ");
            sql.append(
                    parsedQuery.orderByList.stream()
                            .map(o -> prefix + o.field.toUpperCase() + (o.desc ? " DESC" : " ASC"))
                            .collect(Collectors.joining(", "))
            );
        }
    }

    private static void renderColumns(Class<?> entity, String alias, StringJoiner columns) {
        for (Field field : entity.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            if (Objects.equals(alias, "p1")) {
                columns.add(alias + "." + column.name());
            } else {
                columns.add(alias + "." + column.name() + " AS " + alias.toUpperCase() + "_" + column.name());
            }
        }
    }

    private static Class<?> getGenericType(Field field) {
        if (!(field.getGenericType() instanceof ParameterizedType)) {
            return null;
        }
        ParameterizedType type = (ParameterizedType) field.getGenericType();
        return (Class<?>) type.getActualTypeArguments()[0];
    }

    private static String conditionSql(Condition c, String alias, Object[] args, int[] argIndex) {
        String field = (alias == null || alias.isEmpty())
                ? c.field.toUpperCase()
                : alias + "." + c.field.toUpperCase();

        if (c.operator == Operator.IN || c.operator == Operator.NOT_IN) {
            Object param = nextArg(args, argIndex);
            if (!(param instanceof Collection)) {
                throw new IllegalArgumentException("Operador " + c.operator + " exige uma Collection como argumento.");
            }

            Collection<?> collection = (Collection<?>) param;
            if (collection.isEmpty()) {
                return c.operator == Operator.IN ? "1 = 0" : "1 = 1";
            }

            String placeholders = IntStream.range(0, collection.size())
                    .mapToObj(i -> "?")
                    .collect(Collectors.joining(", "));

            String keyword = c.operator == Operator.NOT_IN ? " NOT IN " : " IN ";
            return field + keyword + "(" + placeholders + ")";
        }

        switch (c.operator) {
            case EQ:
                consumeArgs(args, argIndex, 1);
                return field + " = ?";

            case NE:
                consumeArgs(args, argIndex, 1);
                return field + " <> ?";

            case GT:
                consumeArgs(args, argIndex, 1);
                return field + " > ?";

            case GTE:
                consumeArgs(args, argIndex, 1);
                return field + " >= ?";

            case LT:
                consumeArgs(args, argIndex, 1);
                return field + " < ?";

            case LTE:
                consumeArgs(args, argIndex, 1);
                return field + " <= ?";

            case LIKE:
            case CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
                consumeArgs(args, argIndex, 1);
                return field + " LIKE ?";

            case NOT_LIKE:
                consumeArgs(args, argIndex, 1);
                return field + " NOT LIKE ?";

            case BETWEEN:
                consumeArgs(args, argIndex, 2);
                return field + " BETWEEN ? AND ?";

            case IS_NULL:
                return field + " IS NULL";

            case IS_NOT_NULL:
                return field + " IS NOT NULL";

            case TRUE:
                return field + " = TRUE";

            case FALSE:
                return field + " = FALSE";

            default:
                throw new IllegalArgumentException("Operador não suportado: " + c.operator);
        }
    }

    private static Object nextArg(Object[] args, int[] argIndex) {
        if (args == null || argIndex[0] >= args.length) {
            throw new IllegalArgumentException("Quantidade de argumentos insuficiente para a consulta.");
        }
        Object value = args[argIndex[0]];
        argIndex[0]++;
        return value;
    }

    private static void consumeArgs(Object[] args, int[] argIndex, int count) {
        for (int i = 0; i < count; i++) {
            nextArg(args, argIndex);
        }
    }

}
