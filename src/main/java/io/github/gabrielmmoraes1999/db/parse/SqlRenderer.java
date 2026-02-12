package io.github.gabrielmmoraes1999.db.parse;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.Table;

import java.lang.reflect.Field;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class SqlRenderer {

    private static final String DEFAULT_ALIAS = "p1";

    public static <T> String toSql(ParsedQuery parsedQuery, Class<T> entityClass) {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table");
        }

        StringBuilder sql = new StringBuilder();
        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        switch (parsedQuery.type) {

            case SELECT:
                StringJoiner columns = new StringJoiner(", ");

                for (Field field : entityClass.getDeclaredFields()) {
                    if (!field.isAnnotationPresent(Column.class)) {
                        continue;
                    }

                    Column column = field.getAnnotation(Column.class);
                    columns.add(String.join(".", DEFAULT_ALIAS, column.name()));
                }

                sql.append("SELECT ")
                        .append(columns)
                        .append(" FROM ")
                        .append(tableName)
                        .append(" ")
                        .append(DEFAULT_ALIAS);
                break;

            case COUNT:
                sql.append("SELECT COUNT(*) FROM ").append(tableName);
                break;

            case EXISTS:
                sql.append("SELECT 1 FROM ").append(tableName);
                break;

            case DELETE:
                sql.append("DELETE FROM ").append(tableName);
                break;
        }

        if (!parsedQuery.orGroups.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(
                    parsedQuery.orGroups.stream()
                            .map(group ->
                                    group.stream()
                                            .map(SqlRenderer::conditionSql)
                                            .collect(Collectors.joining(" AND "))
                            )
                            .collect(Collectors.joining(" OR "))
            );
        }

        if (parsedQuery.type == QueryType.SELECT && !parsedQuery.orderByList.isEmpty()) {
            sql.append(" ORDER BY ");
            sql.append(
                    parsedQuery.orderByList.stream()
                            .map(o -> DEFAULT_ALIAS + "." + o.field + (o.desc ? " DESC" : " ASC"))
                            .collect(Collectors.joining(", "))
            );
        }

        return sql.toString();
    }


    private static String conditionSql(Condition c) {
        String field = DEFAULT_ALIAS + "." + c.field.toUpperCase();

        switch (c.operator) {

            case EQ:
                return field + " = ?";

            case NE:
                return field + " <> ?";

            case GT:
                return field + " > ?";

            case GTE:
                return field + " >= ?";

            case LT:
                return field + " < ?";

            case LTE:
                return field + " <= ?";

            case LIKE:
            case CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
                return field + " LIKE ?";

            case NOT_LIKE:
                return field + " NOT LIKE ?";

            case IN:
                return field + " IN (?)";

            case NOT_IN:
                return field + " NOT IN (?)";

            case BETWEEN:
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

}
