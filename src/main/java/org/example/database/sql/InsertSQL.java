package org.example.database.sql;

import org.example.database.annotation.Column;
import org.example.database.annotation.IgnoreSQL;
import org.example.database.annotation.Table;

import java.lang.reflect.Field;

public class InsertSQL {

    private final Object entity;

    public InsertSQL(Object entity) {
        this.entity = entity;
    }

    public String get() throws IllegalAccessException {
        Class<?> clazz = entity.getClass();

        // Verifica se a classe tem a anotação @Table
        if (!clazz.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        Table table = clazz.getAnnotation(Table.class);
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(table.name()).append(" (");

        Field[] fields = clazz.getDeclaredFields();
        boolean firstField = true;
        for (Field field : fields) {
            if (field.isAnnotationPresent(Column.class) && !field.isAnnotationPresent(IgnoreSQL.class)) {
                if (!firstField) {
                    sql.append(", ");
                }
                Column column = field.getAnnotation(Column.class);
                sql.append(column.name());
                firstField = false;
            }
        }

        sql.append(") VALUES (");

        firstField = true;
        for (Field field : fields) {
            if (field.isAnnotationPresent(Column.class) && !field.isAnnotationPresent(IgnoreSQL.class)) {
                if (!firstField) {
                    sql.append(", ");
                }

                sql.append("?");
                firstField = false;
            }
        }

        sql.append(");");
        return sql.toString();
    }
}
