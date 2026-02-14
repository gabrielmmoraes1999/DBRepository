package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Param;
import io.github.gabrielmmoraes1999.db.annotation.Query;
import io.github.gabrielmmoraes1999.db.annotation.Table;
import io.github.gabrielmmoraes1999.db.core.EntityBuilder;
import io.github.gabrielmmoraes1999.db.parse.SqlTemplate;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.*;
import java.util.*;

public class DQLCustom {

    @Deprecated
    public static <T> T getEntity(Class<T> entityClass, Method method, Object[] args, Connection connection) throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException, InstantiationException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        List<T> results;
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            results = EntityBuilder.build(entityClass, preparedStatement);
        }

        if (results.isEmpty()) {
            return null;
        } else {
            return results.get(0);
        }
    }

    @Deprecated
    public static <T> List<T> getEntityList(Class<T> entityClass, Method method, Object[] args, Connection connection) throws SQLException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        List<T> results;
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            results = EntityBuilder.build(entityClass, preparedStatement);
        }

        return results;
    }

    public static Map<String, Object> getMap(Method method, Object[] args, Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());
        Map<String, Object> result = new LinkedHashMap<>();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                if (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object columnValue = resultSet.getObject(i);
                        result.put(columnName, columnValue);
                    }
                }
            }

        }

        return result;
    }

    public static List<Map<String, Object>> getMapList(Method method, Object[] args, Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());
        List<Map<String, Object>> resultList = new ArrayList<>();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    Map<String, Object> result = new LinkedHashMap<>();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object columnValue = resultSet.getObject(i);
                        result.put(columnName, columnValue);
                    }

                    resultList.add(result);
                }
            }

        }

        return resultList;
    }

    public static JSONObject getJsonObject(Method method, Object[] args, Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());
        JSONObject result = new JSONObject();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                if (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object columnValue = resultSet.getObject(i);
                        result.put(columnName, columnValue);
                    }
                }
            }

        }

        return result;
    }

    public static JSONArray getJsonArray(Method method, Object[] args, Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());
        JSONArray resultList = new JSONArray();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    JSONObject result = new JSONObject();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object columnValue = resultSet.getObject(i);
                        result.put(columnName, columnValue);
                    }

                    resultList.put(result);
                }
            }

        }

        return resultList;
    }

    public static <R> R getObject(Class<R> returnType, Method method, Object[] args, Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());
        R result = null;

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    result = resultSet.getObject(1, returnType);
                }
            }

        }

        return result;
    }

    public static <R> List<R> getObjectList(Class<R> classList, Method method, Object[] args, Connection connection) throws SQLException {
        Query queryAnnotation = method.getAnnotation(Query.class);
        Parameter[] parameters = method.getParameters();
        SqlTemplate sqlTemplate = SqlTemplate.of(queryAnnotation.value());
        List<R> resultList = new ArrayList<>();

        if (args != null) {
            for (int index = 0; index < args.length; index++) {
                Param paramAnnotation = parameters[index].getAnnotation(Param.class);

                if (paramAnnotation != null) {
                    String paramName = paramAnnotation.value();
                    sqlTemplate.set(paramName, args[index]);
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlTemplate.getSql())) {
            List<Object> bindValues = sqlTemplate.getBindValues();

            int position = 1;
            for (Object object : bindValues) {
                SQLUtils.setPreparedStatement(preparedStatement, position, object);
                position++;
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    resultList.add(resultSet.getObject(1, classList));
                }
            }

        }

        return resultList;
    }

}
