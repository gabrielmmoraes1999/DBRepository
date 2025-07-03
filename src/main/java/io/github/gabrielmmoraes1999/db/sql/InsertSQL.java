package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.Table;

import java.lang.reflect.Field;

public class InsertSQL {

    private final Object entity;

    public InsertSQL(Object entity) {
        this.entity = entity;
    }

    public String get() throws IllegalAccessException {
        Class<?> clazz = entity.getClass();

        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        Table table = clazz.getAnnotation(Table.class);
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(table.name()).append(" (");

        StringBuilder valuesPart = new StringBuilder(") VALUES (");

        Field[] fields = clazz.getDeclaredFields();
        boolean firstField = true;

        for (Field field : fields) {
            if (field.isAnnotationPresent(Column.class)) {
                field.setAccessible(true);

                if (field.get(entity) != null) {
                    if (!firstField) {
                        sql.append(", ");
                        valuesPart.append(", ");
                    }

                    Column column = field.getAnnotation(Column.class);
                    sql.append(column.name());
                    valuesPart.append("?");
                    firstField = false;
                }
            }
        }

        sql.append(valuesPart).append(");");
        return sql.toString();
    }
}
