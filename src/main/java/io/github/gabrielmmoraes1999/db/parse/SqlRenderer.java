package io.github.gabrielmmoraes1999.db.parse;

import java.util.stream.Collectors;

public class SqlRenderer {

    public static String toSql(ParsedQuery q, String table) {

        StringBuilder sql = new StringBuilder();

        switch (q.type) {

            case SELECT:
                sql.append("SELECT * FROM ").append(table);
                break;

            case COUNT:
                sql.append("SELECT COUNT(*) FROM ").append(table);
                break;

            case EXISTS:
                sql.append("SELECT 1 FROM ").append(table);
                break;

            case DELETE:
                sql.append("DELETE FROM ").append(table);
                break;
        }

        if (!q.orGroups.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(
                    q.orGroups.stream()
                            .map(group ->
                                    group.stream()
                                            .map(SqlRenderer::conditionSql)
                                            .collect(Collectors.joining(" AND "))
                            )
                            .collect(Collectors.joining(" OR "))
            );
        }

        if (q.type == QueryType.SELECT && !q.orderByList.isEmpty()) {
            sql.append(" ORDER BY ");
            sql.append(
                    q.orderByList.stream()
                            .map(o -> o.field + (o.desc ? " DESC" : " ASC"))
                            .collect(Collectors.joining(", "))
            );
        }

        return sql.toString();
    }


    private static String conditionSql(Condition c) {

        switch (c.operator) {

            case EQ:
                return c.field + " = ?";

            case NE:
                return c.field + " <> ?";

            case GT:
                return c.field + " > ?";

            case GTE:
                return c.field + " >= ?";

            case LT:
                return c.field + " < ?";

            case LTE:
                return c.field + " <= ?";

            case LIKE:
            case CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
                return c.field + " LIKE ?";

            case NOT_LIKE:
                return c.field + " NOT LIKE ?";

            case IN:
                return c.field + " IN (?)";

            case NOT_IN:
                return c.field + " NOT IN (?)";

            case BETWEEN:
                return c.field + " BETWEEN ? AND ?";

            case IS_NULL:
                return c.field + " IS NULL";

            case IS_NOT_NULL:
                return c.field + " IS NOT NULL";

            case TRUE:
                return c.field + " = TRUE";

            case FALSE:
                return c.field + " = FALSE";

            default:
                throw new IllegalArgumentException(
                        "Operador não suportado: " + c.operator
                );
        }
    }
}
