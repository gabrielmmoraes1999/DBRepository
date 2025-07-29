package io.github.gabrielmmoraes1999.db.util;

import io.github.gabrielmmoraes1999.db.TypeSQL;
import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.OneToMany;
import io.github.gabrielmmoraes1999.db.annotation.Table;
import io.github.gabrielmmoraes1999.db.sql.DeleteSQL;
import io.github.gabrielmmoraes1999.db.sql.InsertSQL;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.*;

public class SubClass {

    public static void execute(Object entity, TypeSQL typeSQL, Connection connection) throws Exception {
        Class<?> entityClass = entity.getClass();
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(OneToMany.class)) {
                field.setAccessible(true);
                Collection<?> children = (Collection<?>) field.get(entity);
                Class<?> fieldClazz = Function.getGenericType(field);
                if (children == null) continue;

                if (!fieldClazz.isAnnotationPresent(Table.class)) {
                    throw new IllegalArgumentException("The class does not have the annotation @Table.");
                }

                Table tableAnnotation = Objects.requireNonNull(fieldClazz.getAnnotation(Table.class));
                Map<String, String> fkMap = Function.resolveJoinColumnsOrFKMeta(connection, table.name(), tableAnnotation.name(), field);
                Map<String, Object> valuesPk = new HashMap<>();
                List<Object> values = new ArrayList<>();

                for (String pkColumn : fkMap.keySet()) {
                    for (Field field1 : entityClass.getDeclaredFields()) {
                        if (field1.isAnnotationPresent(Column.class)) {
                            Column column = Objects.requireNonNull(field1.getAnnotation(Column.class));

                            if (column.name().equalsIgnoreCase(pkColumn)) {
                                field1.setAccessible(true);
                                Object value = field1.get(entity);
                                valuesPk.put(fkMap.get(pkColumn).toUpperCase(), value);
                                values.add(value);
                            }
                        }
                    }
                }

                Set<String> keys = valuesPk.keySet();

                if (TypeSQL.INSERT != typeSQL) {
                    Function.executeSQL(new DeleteSQL(fieldClazz).get(fkMap.keySet()), values, connection);
                }

                for (Object child : children) {
                    for (Field field1 : child.getClass().getDeclaredFields()) {
                        Column parentColumn = field1.getAnnotation(Column.class);

                        if (parentColumn != null) {
                            String columnStr = parentColumn.name().toUpperCase();

                            if (keys.contains(columnStr)) {
                                field1.setAccessible(true);
                                field1.set(child, valuesPk.get(columnStr));
                            }
                        }
                    }

                    Function.execute(child, new InsertSQL(child).get(), TypeSQL.INSERT, connection);
                }
            }
        }
    }

}
