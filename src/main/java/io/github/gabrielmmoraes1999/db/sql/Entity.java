package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

class Entity {

    protected static <T> T build(Class<T> entityClass, ResultSet resultSet, Connection connection) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        T entity = entityClass.getDeclaredConstructor().newInstance();

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            field.setAccessible(true);
            if (field.getType().isEnum()) {
                assert column != null;
                field.set(entity, field.getType().getEnumConstants()[resultSet.getInt(column.name())]);
            } else {
                field.set(entity, resultSet.getObject(column.name()));
            }
        }

        Entity.loadOneToOne(entity, connection);
        Entity.loadOneToMany(entity, connection);
        return entity;
    }

    private static void loadOneToMany(Object parent, Connection connection) throws IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        Class<?> parentClass = parent.getClass();

        for (Field field : parentClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(OneToMany.class)) {
                continue;
            }

            field.setAccessible(true);

            JoinColumns joinColumns = field.getAnnotation(JoinColumns.class);
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);

            List<JoinColumn> joins = new ArrayList<>();

            if (joinColumns != null) {
                joins.addAll(Arrays.asList(joinColumns.value()));
            } else if (joinColumn != null) {
                joins.add(joinColumn);
            } else {
                throw new IllegalArgumentException("@OneToOne sem @JoinColumn(s): " + field.getName());
            }

            Class<?> childClass = SQLUtils.getCollectionGenericType(field);

            if (!childClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("Classe filha sem @Table");
            }

            Table table = childClass.getAnnotation(Table.class);
            String tableName = table.name();

            // SELECT colunas
            StringJoiner select = new StringJoiner(", ");
            for (Field f : childClass.getDeclaredFields()) {
                if (f.isAnnotationPresent(Column.class)) {
                    select.add(f.getAnnotation(Column.class).name());
                }
            }

            // WHERE com JoinColumns
            StringJoiner where = new StringJoiner(" AND ");
            List<Object> params = new ArrayList<>();

            for (JoinColumn jc : joins) {
                Field parentField = SQLUtils.findFieldByColumn(parentClass, jc.referencedColumnName());

                if (parentField == null) {
                    throw new RuntimeException("Campo não encontrado: " + jc.referencedColumnName());
                }

                parentField.setAccessible(true);
                Object value = parentField.get(parent);

                where.add(jc.name() + " = ?");
                params.add(value);
            }

            List<Object> children = new ArrayList<>();
            String sql = String.format("SELECT %s FROM %s WHERE %s", select, tableName, where);

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Object child = childClass.getDeclaredConstructor().newInstance();

                        for (Field f : childClass.getDeclaredFields()) {
                            if (!f.isAnnotationPresent(Column.class)) {
                                continue;
                            }

                            Column col = f.getAnnotation(Column.class);
                            f.setAccessible(true);
                            f.set(child, rs.getObject(col.name()));
                        }

                        children.add(child);
                    }
                }
            }

            field.set(parent, children);
        }
    }

    private static void loadOneToOne(Object parent, Connection connection) throws IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        Class<?> parentClass = parent.getClass();

        for (Field field : parentClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(OneToOne.class)) {
                continue;
            }

            field.setAccessible(true);

            // Suporta @JoinColumn ou @JoinColumns
            JoinColumns joinColumns = field.getAnnotation(JoinColumns.class);
            JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);

            List<JoinColumn> joins = new ArrayList<>();

            if (joinColumns != null) {
                joins.addAll(Arrays.asList(joinColumns.value()));
            } else if (joinColumn != null) {
                joins.add(joinColumn);
            } else {
                throw new IllegalArgumentException("@OneToOne sem @JoinColumn(s): " + field.getName());
            }

            Class<?> targetClass = field.getType();

            if (!targetClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("Classe OneToOne sem @Table: " + targetClass.getName());
            }

            Table table = targetClass.getAnnotation(Table.class);
            String tableName = table.name();

            // SELECT colunas
            StringJoiner select = new StringJoiner(", ");
            for (Field f : targetClass.getDeclaredFields()) {
                if (f.isAnnotationPresent(Column.class)) {
                    select.add(f.getAnnotation(Column.class).name());
                }
            }

            // WHERE
            StringJoiner where = new StringJoiner(" AND ");
            List<Object> params = new ArrayList<>();

            for (JoinColumn jc : joins) {
                Field parentField = SQLUtils.findFieldByColumn(parentClass, jc.referencedColumnName());

                if (parentField == null) {
                    throw new RuntimeException("Campo não encontrado: " + jc.referencedColumnName());
                }

                parentField.setAccessible(true);
                Object value = parentField.get(parent);

                where.add(jc.name() + " = ?");
                params.add(value);
            }

            String sql = String.format("SELECT %s FROM %s WHERE %s", select, tableName, where);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {

                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }

                try (ResultSet rs = ps.executeQuery()) {

                    if (rs.next()) {
                        Object target = targetClass
                                .getDeclaredConstructor()
                                .newInstance();

                        for (Field f : targetClass.getDeclaredFields()) {
                            if (!f.isAnnotationPresent(Column.class)) {
                                continue;
                            }

                            Column col = f.getAnnotation(Column.class);
                            f.setAccessible(true);
                            f.set(target, rs.getObject(col.name()));
                        }

                        field.set(parent, target);
                    } else {
                        field.set(parent, null);
                    }
                }
            }
        }
    }

}
