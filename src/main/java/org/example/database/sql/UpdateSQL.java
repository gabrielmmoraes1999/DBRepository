package org.example.database.sql;

import org.example.database.annotation.Column;
import org.example.database.annotation.PrimaryKey;
import org.example.database.annotation.Table;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class UpdateSQL {

    private final Object entity;

    public UpdateSQL(Object entity) {
        this.entity = entity;
    }

    public String get() throws IllegalAccessException {
        Class<?> entityClass = entity.getClass();

        Table table = entityClass.getAnnotation(Table.class);
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ");
        sqlBuilder.append(table.name()).append(" SET ");

        List<Field> fieldList = new ArrayList<>();
        StringBuilder setClause = new StringBuilder();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                fieldList.add(field);
                continue;
            }

            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                setClause.append(column.name()).append(" = ?, ");
            }
        }

        // Remover a última vírgula e espaço extra
        if (setClause.length() > 0) {
            setClause.setLength(setClause.length() - 2);
        }

        sqlBuilder.append(setClause).append(" WHERE ");

        fieldList.forEach(primaryKey -> {
            Column primaryKeyColumn = primaryKey.getAnnotation(Column.class);
            sqlBuilder.append(primaryKeyColumn.name()).append(" = ? AND ");
        });

        if (sqlBuilder.length() > 0) {
            sqlBuilder.setLength(sqlBuilder.length() - 5);
        }

        sqlBuilder.append(";");
        return sqlBuilder.toString();
    }
}

