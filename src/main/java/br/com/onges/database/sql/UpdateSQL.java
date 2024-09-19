package br.com.onges.database.sql;

import br.com.onges.database.annotation.Column;
import br.com.onges.database.annotation.PrimaryKey;
import br.com.onges.database.annotation.Table;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class UpdateSQL {

    private final Object entity;

    public UpdateSQL(Object entity) {
        this.entity = entity;
    }

    public String get() throws IllegalAccessException {
        Class<?> entityClass = entity.getClass();
        boolean isNull = true;

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
                field.setAccessible(true);
                Object value = field.get(entity);

                if (Objects.isNull(value)) {
                    continue;
                }

                isNull = false;
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

        if (isNull) {
            return null;
        }

        sqlBuilder.append(";");
        return sqlBuilder.toString();
    }
}

