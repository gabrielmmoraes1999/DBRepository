package io.github.gabrielmmoraes1999.db.core;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.sql.SQLUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

public class EntityBuilder {

    public static <T> List<T> build(Class<T> entityClass, PreparedStatement preparedStatement, Connection connection) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        List<T> entityList = new ArrayList<>();

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
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

                entityList.add(entity);
            }
        }

        preparedStatement.close();

        for (T entity : entityList) {
            EntityBuilder.loadOneToOne(entity, connection);
            EntityBuilder.loadOneToMany(entity, connection);
        }

        return entityList;
    }

    public static <T> List<T> build(Class<T> entityClass, PreparedStatement preparedStatement) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Map<String, T> rootMap = new LinkedHashMap<>();
        Map<Field, String> joinAliases = new LinkedHashMap<>();

        // Processa OneToMany
        int aliasCounter = 2;
        for (Field field : entityClass.getDeclaredFields()) {
            Class<?> targetEntity;

            if (field.isAnnotationPresent(OneToOne.class)) {
                targetEntity = field.getType();
            } else if (field.isAnnotationPresent(OneToMany.class)) {
                targetEntity = getGenericType(field);
            } else {
                continue;
            }

            if (targetEntity == null) {
                continue;
            }

            joinAliases.put(field, "p" + aliasCounter++);
        }

        try (ResultSet rs = preparedStatement.executeQuery()) {
            while (rs.next()) {
                String rootKey = extractPrimaryKey(entityClass, rs, "p1");

                T root = rootMap.get(rootKey);
                if (root == null) {
                    root = entityClass.getDeclaredConstructor().newInstance();
                    populateEntity(root, entityClass, rs, "p1");
                    rootMap.put(rootKey, root);
                }

                // Processa OneToMany
                for (Field field : entityClass.getDeclaredFields()) {
                    Class<?> targetEntity;

                    if (field.isAnnotationPresent(OneToOne.class)) {
                        targetEntity = field.getType();
                    } else if (field.isAnnotationPresent(OneToMany.class)) {
                        targetEntity = getGenericType(field);
                    } else {
                        continue;
                    }

                    String alias = joinAliases.get(field);
                    if (alias == null) {
                        continue;
                    }

                    Object child = targetEntity.getDeclaredConstructor().newInstance();
                    boolean hasValue = populateEntity(child, targetEntity, rs, alias);

                    if (!hasValue) {
                        continue;
                    }

                    field.setAccessible(true);
                    if (Collection.class.isAssignableFrom(field.getType())) {
                        List<Object> list = (List<Object>) field.get(root);

                        if (list == null) {
                            list = new ArrayList<>();
                            field.set(root, list);
                        }

                        if (!containsEntity(list, child)) {
                            list.add(child);
                        }
                    } else {
                        field.set(root, child);
                    }
                }
            }
        }

        return new ArrayList<>(rootMap.values());
    }

    private static boolean populateEntity(Object entity, Class<?> clazz, ResultSet rs, String alias) throws SQLException, IllegalAccessException {
        boolean hasNonNull = false;

        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            String columnName = alias.equals("p1")
                    ? column.name()
                    : alias.toUpperCase() + "_" + column.name();

            Object value = rs.getObject(columnName);
            if (value != null) {
                hasNonNull = true;
            }

            field.setAccessible(true);
            field.set(entity, value);
        }

        return hasNonNull;
    }

    private static String extractPrimaryKey(Class<?> clazz, ResultSet rs, String alias) throws SQLException {

        StringBuilder key = new StringBuilder();

        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isAnnotationPresent(PrimaryKey.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            String col = alias.equals("p1")
                    ? column.name()
                    : alias.toUpperCase() + "_" + column.name();

            key.append(rs.getObject(col)).append("|");
        }

        return key.toString();
    }

    private static boolean containsEntity(List<?> list, Object entity) {
        return list.stream().anyMatch(e -> e.equals(entity));
    }

    private static Class<?> getGenericType(Field field) {

        if (!(field.getGenericType() instanceof ParameterizedType)) {
            return null;
        }

        ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();

        if (parameterizedType.getActualTypeArguments().length == 0) {
            return null;
        }

        Type typeArgument = parameterizedType.getActualTypeArguments()[0];

        if (typeArgument instanceof Class<?>) {
            return (Class<?>) typeArgument;
        }

        if (typeArgument instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) typeArgument).getRawType();
        }

        return null;
    }

    private static <T> void loadOneToOne(T entity, Connection connection) throws IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        Class<?> parentClass = entity.getClass();

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
                Object value = parentField.get(entity);

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

                        field.set(entity, target);
                    } else {
                        field.set(entity, null);
                    }
                }
            }
        }
    }

    private static <T> void loadOneToMany(T entity, Connection connection) throws IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        Class<?> parentClass = entity.getClass();

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
                Object value = parentField.get(entity);

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

            field.set(entity, children);
        }
    }

}
