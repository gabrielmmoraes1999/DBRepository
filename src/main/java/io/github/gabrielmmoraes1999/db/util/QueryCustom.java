package io.github.gabrielmmoraes1999.db.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class QueryCustom {

    public static <T> T getEntity(PreparedStatement preparedStatement, Class<T> entityClass) {
        T entity = null;

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    entity = Function.getEntity(entityClass, resultSet);
                }
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
                if (resultSet.next()) {
                    int columnCount = resultSet.getMetaData().getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = resultSet.getMetaData().getColumnLabel(i);
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

    public static <T> List<T> getEntityList(PreparedStatement preparedStatement, Class<T> entityClass) {
        List<T> resultList = new ArrayList<>();

        try {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    resultList.add(Function.getEntity(entityClass, resultSet));
                }
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
                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    int columnCount = resultSet.getMetaData().getColumnCount();

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = resultSet.getMetaData().getColumnLabel(i);
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

}
