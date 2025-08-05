package io.github.gabrielmmoraes1999.db.util;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryCustom {

    public static <T> T getEntity(PreparedStatement preparedStatement, Class<T> entityClass, Connection connection) {
        T entity = null;

        try {
            List<Map<String, Object>> childList = Function.convertResultSetToMap(preparedStatement.executeQuery());

            if (!childList.isEmpty()) {
                entity = Function.getEntity(entityClass, childList.get(0), connection);
            }

            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entity;
    }

    public static <R> R getObject(PreparedStatement preparedStatement, Class<R> returnType) {
        R obj = null;

        try {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    obj = resultSet.getObject(1, returnType);
                }
            }
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    public static Map<String, Object> getMap(PreparedStatement preparedStatement) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {

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

            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static <T> List<T> getEntityList(PreparedStatement preparedStatement, Class<T> entityClass, Connection connection) {
        List<T> resultList = new ArrayList<>();

        try {
            for (Map<String, Object> row : Function.convertResultSetToMap(preparedStatement.executeQuery())) {
                resultList.add(Function.getEntity(entityClass, row, connection));
            }

            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    public static <R> List<R> getObjectList(PreparedStatement preparedStatement, Class<R> classList) {
        List<R> resultList = new ArrayList<>();

        try {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    resultList.add(resultSet.getObject(1, classList));
                }
            }
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    public static List<Map<String, Object>> getMapList(PreparedStatement preparedStatement) {
        List<Map<String, Object>> resultList = new ArrayList<>();

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object columnValue = resultSet.getObject(i);
                        row.put(columnName, columnValue);
                    }

                    resultList.add(row);
                }
            }

            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    public static JSONObject getJsonObject(PreparedStatement preparedStatement) {
        JSONObject jsonObject = new JSONObject();

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                if (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object value = resultSet.getObject(i);
                        jsonObject.put(columnName, value);
                    }
                }
            }

            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return jsonObject;
    }

    public static JSONArray getJsonArray(PreparedStatement preparedStatement) {
        JSONArray jsonArray = new JSONArray();

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    JSONObject jsonObject = new JSONObject();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object value = resultSet.getObject(i);
                        jsonObject.put(columnName, value);
                    }

                    jsonArray.put(jsonObject);
                }
            }

            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return jsonArray;
    }

}
