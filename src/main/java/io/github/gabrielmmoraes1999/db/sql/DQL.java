package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.core.EntityBuilder;
import io.github.gabrielmmoraes1999.db.parse.MethodNameParser;
import io.github.gabrielmmoraes1999.db.parse.ParsedQuery;
import io.github.gabrielmmoraes1999.db.parse.QueryType;
import io.github.gabrielmmoraes1999.db.parse.SqlRenderer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;

public class DQL {

    public static <T> List<T> findAll(Class<T> entityClass, Connection connection) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        List<T> resultList;

        try (PreparedStatement preparedStatement = connection.prepareStatement(SqlRenderer.toSql(null, null, entityClass))) {
            resultList = EntityBuilder.build(entityClass, preparedStatement);
        }

        return resultList;
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

        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }

            Column column = field.getAnnotation(Column.class);
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                field.setAccessible(true);
                if (Objects.isNull(field.get(entity))) {
                    whereClause.add(String.format("p1.%s IS NULL", column.name()));
                } else {
                    whereClause.add(String.format("p1.%s = ?", column.name()));
                    primaryKeyFields.add(field);
                }
            }
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("Class without @PrimaryKey");
        }

        List<T> result;
        String sql = String.format("%s WHERE %s", SqlRenderer.toSql(null, null, entityClass), whereClause);
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int position = 1;
            for (Field field : primaryKeyFields) {
                field.setAccessible(true);
                position = SQLUtils.setPreparedStatement(preparedStatement, position, field.get(entity));
            }

            result = EntityBuilder.build(entityClass, preparedStatement);
        }

        if (result.isEmpty()) {
            return null;
        } else {
            return result.get(0);
        }
    }

    public static <T> Object handleMethod(Class<T> entityClass, Method method, Object[] args, Connection connection) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String methodName = method.getName();
        Class<?> returnClass = method.getReturnType();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        Object[] safeArgs = args != null ? args : new Object[0];
        ParsedQuery query = MethodNameParser.parse(methodName);

        if (query.type == QueryType.COUNT) {
            return executeCount(entityClass, query, safeArgs, connection, returnClass);
        }

        if (query.type == QueryType.EXISTS) {
            return executeExists(entityClass, query, safeArgs, connection, returnClass);
        }

        if (query.type == QueryType.DELETE) {
            return executeDelete(entityClass, query, safeArgs, connection, returnClass);
        }

        return executeSelect(entityClass, query, safeArgs, connection, returnClass);
    }

    private static <T> Object executeSelect(Class<T> entityClass, ParsedQuery query, Object[] args, Connection connection, Class<?> returnClass) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        T resultClass = null;
        List<T> resultList = new ArrayList<>();
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();

        try (PreparedStatement preparedStatement = connection.prepareStatement(SqlRenderer.toSql(query, args, entityClass))) {
            bindArgs(preparedStatement, args);

            if (returnClass.isAssignableFrom(JSONObject.class) || returnClass.isAssignableFrom(JSONArray.class)) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    if (returnClass.isAssignableFrom(JSONObject.class)) {
                        if (resultSet.next()) {
                            for (int i = 1; i <= columnCount; i++) {
                                jsonObject.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                            }
                        }
                    } else if (returnClass.isAssignableFrom(JSONArray.class)) {
                        while (resultSet.next()) {
                            JSONObject jsonObjectProp = new JSONObject();

                            for (int i = 1; i <= columnCount; i++) {
                                jsonObjectProp.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                            }

                            jsonArray.put(jsonObjectProp);
                        }
                    }
                }
            } else if (returnClass.isAssignableFrom(List.class)) {
                resultList = EntityBuilder.build(entityClass, preparedStatement);
            } else if (returnClass.isAssignableFrom(entityClass)) {
                List<T> entityList = EntityBuilder.build(entityClass, preparedStatement);

                if (!entityList.isEmpty()) {
                    resultClass = entityList.get(0);
                }
            }
        }

        if (returnClass.isAssignableFrom(List.class)) {
            return resultList;
        } else if (returnClass.isAssignableFrom(entityClass)) {
            return resultClass;
        } else if (returnClass.isAssignableFrom(JSONObject.class)) {
            return jsonObject;
        } else if (returnClass.isAssignableFrom(JSONArray.class)) {
            return jsonArray;
        } else {
            return null;
        }
    }

    private static <T> Object executeCount(Class<T> entityClass, ParsedQuery query, Object[] args, Connection connection, Class<?> returnClass) throws SQLException {
        long count;
        try (PreparedStatement preparedStatement = connection.prepareStatement(SqlRenderer.toSql(query, args, entityClass))) {
            bindArgs(preparedStatement, args);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                resultSet.next();
                count = resultSet.getLong(1);
            }
        }

        if (returnClass == long.class || returnClass == Long.class) {
            return count;
        }
        if (returnClass == int.class || returnClass == Integer.class) {
            return (int) count;
        }
        return count;
    }

    private static <T> Object executeExists(Class<T> entityClass, ParsedQuery query, Object[] args, Connection connection, Class<?> returnClass) throws SQLException {
        long count;
        try (PreparedStatement preparedStatement = connection.prepareStatement(SqlRenderer.toSql(query, args, entityClass))) {
            bindArgs(preparedStatement, args);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                resultSet.next();
                count = resultSet.getLong(1);
            }
        }

        boolean exists = count > 0;
        if (returnClass == boolean.class || returnClass == Boolean.class) {
            return exists;
        }
        return exists;
    }

    private static <T> Object executeDelete(Class<T> entityClass, ParsedQuery query, Object[] args, Connection connection, Class<?> returnClass) throws SQLException {
        int deleted;
        try (PreparedStatement preparedStatement = connection.prepareStatement(SqlRenderer.toSql(query, args, entityClass))) {
            bindArgs(preparedStatement, args);
            deleted = preparedStatement.executeUpdate();
        }

        if (returnClass == void.class || returnClass == Void.class) {
            return null;
        }
        if (returnClass == long.class || returnClass == Long.class) {
            return (long) deleted;
        }
        return deleted;
    }

    private static void bindArgs(PreparedStatement preparedStatement, Object[] args) throws SQLException {
        int position = 1;
        for (Object arg : args) {
            position = SQLUtils.setPreparedStatement(preparedStatement, position, arg);
        }
    }

}
