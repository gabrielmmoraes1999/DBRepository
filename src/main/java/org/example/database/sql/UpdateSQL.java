package org.example.database.sql;

import org.example.database.annotation.Column;
import org.example.database.annotation.PrimaryKey;
import org.example.database.annotation.Table;

import java.lang.reflect.Field;

public class UpdateSQL {

    private final String sql;

    public UpdateSQL(Object entity) throws IllegalAccessException {
        Class<?> entityClass = entity.getClass();

        Table table = entityClass.getAnnotation(Table.class);
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ");
        sqlBuilder.append(table.name()).append(" SET ");

        Field primaryKeyField = null;
        StringBuilder setClause = new StringBuilder();
        Object idValue = null;

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKeyField = field;
            }
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                field.setAccessible(true);
                Object value = field.get(entity);

                if (primaryKeyField != null && primaryKeyField.equals(field)) {
                    idValue = value;
                } else {
                    setClause.append(column.name()).append(" = ");
                    if (value instanceof String || value instanceof java.sql.Date) {
                        setClause.append("'").append(value).append("', ");
                    } else {
                        setClause.append(value).append(", ");
                    }
                }
            }
        }

        // Remover a última vírgula e espaço extra
        if (setClause.length() > 0) {
            setClause.setLength(setClause.length() - 2);
        }

        sqlBuilder.append(setClause).append(" WHERE ");

        assert primaryKeyField != null;
        Column primaryKeyColumn = primaryKeyField.getAnnotation(Column.class);
        sqlBuilder.append(primaryKeyColumn.name()).append(" = ");

        if (idValue instanceof String || idValue instanceof java.sql.Date) {
            sqlBuilder.append("'").append(idValue).append("'");
        } else {
            sqlBuilder.append(idValue);
        }

        this.sql = sqlBuilder.toString();
    }

    public String get() {
        return this.sql;
    }
}

