package io.github.gabrielmmoraes1999.db.core;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.OneToMany;
import io.github.gabrielmmoraes1999.db.annotation.OneToOne;
import io.github.gabrielmmoraes1999.db.annotation.PrimaryKey;
import io.github.gabrielmmoraes1999.db.util.Function;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

public class EntityBuilder {

    public static <T> List<T> build(Class<T> entityClass, PreparedStatement preparedStatement) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Map<String, T> rootMap = new LinkedHashMap<>();
        Map<Field, String> joinAliases = new LinkedHashMap<>();

        int aliasCounter = 2;
        for (Field field : entityClass.getDeclaredFields()) {
            Class<?> targetEntity;

            if (field.isAnnotationPresent(OneToOne.class)) {
                targetEntity = field.getType();
            } else if (field.isAnnotationPresent(OneToMany.class)) {
                targetEntity = Function.getGenericType(field);
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

                for (Field field : entityClass.getDeclaredFields()) {
                    Class<?> targetEntity;

                    if (field.isAnnotationPresent(OneToOne.class)) {
                        targetEntity = field.getType();
                    } else if (field.isAnnotationPresent(OneToMany.class)) {
                        targetEntity = Function.getGenericType(field);
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
            String columnName = alias.equals("p1") ? column.name() : alias.toUpperCase() + "_" + column.name();

            Object value = rs.getObject(columnName);
            if (value != null) {
                hasNonNull = true;
            }

            field.setAccessible(true);
            if (field.getType().isEnum()) {
                assert column != null;
                field.set(entity, field.getType().getEnumConstants()[rs.getInt(columnName)]);
            } else {
                field.set(entity, value);
            }
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
            String col = alias.equals("p1") ? column.name() : alias.toUpperCase() + "_" + column.name();
            key.append(rs.getObject(col)).append("|");
        }

        return key.toString();
    }

    private static boolean containsEntity(List<?> list, Object candidate) throws IllegalAccessException {
        for (Object existing : list) {
            if (samePrimaryKey(existing, candidate)) {
                return true;
            }
        }
        return false;
    }

    private static boolean samePrimaryKey(Object a, Object b) throws IllegalAccessException {
        Class<?> clazz = a.getClass();

        for (Field field : clazz.getDeclaredFields()) {
            if (!field.isAnnotationPresent(PrimaryKey.class)) {
                continue;
            }

            field.setAccessible(true);

            Object v1 = field.get(a);
            Object v2 = field.get(b);

            if (!Objects.equals(v1, v2)) {
                return false;
            }
        }
        return true;
    }

}
