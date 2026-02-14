package io.github.gabrielmmoraes1999.db.parse;

import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.stream.Collectors;

public class SqlRenderer {

    private static final String ROOT_ALIAS = "p1";

    public static <T> String toSql(ParsedQuery parsedQuery, Class<T> entityClass) {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table");
        }

        StringBuilder sql = new StringBuilder();
        StringBuilder joins = new StringBuilder();
        StringJoiner columns = new StringJoiner(", ");

        int aliasCounter = 2;

        Table table = entityClass.getAnnotation(Table.class);
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

        if (parsedQuery != null) {
            if (!parsedQuery.orGroups.isEmpty()) {
                sql.append(" WHERE ");
                sql.append(
                        parsedQuery.orGroups.stream()
                                .map(group ->
                                        group.stream()
                                                .map(c -> conditionSql(c, ROOT_ALIAS))
                                                .collect(Collectors.joining(" AND "))
                                )
                                .collect(Collectors.joining(" OR "))
                );
            }

            if (parsedQuery.type == QueryType.SELECT && !parsedQuery.orderByList.isEmpty()) {
                sql.append(" ORDER BY ");
                sql.append(
                        parsedQuery.orderByList.stream()
                                .map(o -> ROOT_ALIAS + "." + o.field.toUpperCase() + (o.desc ? " DESC" : " ASC"))
                                .collect(Collectors.joining(", "))
                );
            }
        }

        return sql.toString();
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

    private static String conditionSql(Condition c, String alias) {
        String field = alias + "." + c.field.toUpperCase();

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
