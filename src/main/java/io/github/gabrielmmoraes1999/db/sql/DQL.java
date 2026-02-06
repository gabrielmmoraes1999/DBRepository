package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.PrimaryKey;
import io.github.gabrielmmoraes1999.db.annotation.Table;
import io.github.gabrielmmoraes1999.db.util.Function;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;

public class DQL {

    public static <T> List<T> findAll(Class<T> entityClass, Connection connection) {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        List<T> entityList = new ArrayList<>();
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        String sql = "SELECT * FROM " + table.name();

        try (Statement statement = connection.createStatement()) {
            for (Map<String, Object> row : Function.convertResultSetToMap(statement.executeQuery(sql))) {
                entityList.add(Function.getEntity(entityClass, row, connection));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entityList;
    }

    public static <T, ID> T findById(Class<T> entityClass, ID id, Connection connection) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        T entity = entityClass.getDeclaredConstructor().newInstance();
        int sizePrimatyKey = 0;

        List<Object> keys;
        if (id instanceof List) {
            keys = (List<Object>) id;
        } else {
            keys = Collections.singletonList(id);
        }

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            if (field.isAnnotationPresent(PrimaryKey.class)) {
                field.setAccessible(true);
                field.set(entity, keys.get(sizePrimatyKey));
                sizePrimatyKey++;
            }
        }

        if (!Objects.equals(sizePrimatyKey, keys.size())) {
            throw new IllegalArgumentException("The amount PK invalid.");
        }

        return DQL.findById(entity, connection);
    }

    public static <T> T findById(T entity, Connection connection) throws InvocationTargetException, InstantiationException, NoSuchMethodException, IllegalAccessException, SQLException {
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner selectClause = new StringJoiner(", ");
        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }
            Column column = field.getAnnotation(Column.class);

            if (field.isAnnotationPresent(PrimaryKey.class)) {
                whereClause.add(String.format("%s = ?", column.name()));
                primaryKeyFields.add(field);

                field.setAccessible(true);
                if (Objects.isNull(field.get(entity))) {
                    throw new IllegalArgumentException("Valor do @PrimaryKey é nulo");
                }
            }

            selectClause.add(column.name());
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("Classe sem @PrimaryKey");
        }

        String sql = String.format("SELECT %s FROM %s WHERE %s", selectClause, tableName, whereClause);
        try (PreparedStatement preparedStatement = SQLUtils.getPreparedStatement(sql, entity, primaryKeyFields, connection);
             ResultSet rs = preparedStatement.executeQuery()) {

            if (rs.next()) {
                entity = entityClass.getDeclaredConstructor().newInstance();

                for (Field field : entityClass.getDeclaredFields()) {
                    if (!field.isAnnotationPresent(Column.class)) {
                        continue;
                    }

                    Column column = field.getAnnotation(Column.class);
                    field.setAccessible(true);
                    field.set(entity, rs.getObject(column.name()));
                }

                return entity;
            }
        }

        return null;
    }

}
