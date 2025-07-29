package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.PrimaryKey;
import io.github.gabrielmmoraes1999.db.annotation.Table;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DeleteSQL {

    private final Class<?> entityClass;

    public DeleteSQL(Class<?> entityClass) {
        this.entityClass = entityClass;
    }

    public String get() {
        Table table = entityClass.getAnnotation(Table.class);
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ");
        sqlBuilder.append(table.name());

        List<Field> fieldList = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                fieldList.add(field);
            }
        }

        sqlBuilder.append(" WHERE ");

        fieldList.forEach(primaryKey -> {
            Column primaryKeyColumn = primaryKey.getAnnotation(Column.class);
            assert primaryKeyColumn != null;
            sqlBuilder.append(primaryKeyColumn.name()).append(" = ? AND ");
        });

        if (sqlBuilder.length() > 0) {
            sqlBuilder.setLength(sqlBuilder.length() - 5);
        }

        return sqlBuilder.toString();
    }

    public String get(Set<String> primaryKeys) {
        Table table = entityClass.getAnnotation(Table.class);
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ");
        sqlBuilder.append(table.name()).append(" WHERE ");

        primaryKeys.forEach(primaryKey -> sqlBuilder.append(primaryKey).append(" = ? AND "));

        if (sqlBuilder.length() > 0) {
            sqlBuilder.setLength(sqlBuilder.length() - 5);
        }

        return sqlBuilder.toString();
    }

}
