package org.example.database;

import org.example.database.annotation.Column;
import org.example.database.annotation.PrimaryKey;
import org.example.database.annotation.Query;
import org.example.database.annotation.Table;
import org.example.database.annotation.Update;
import org.example.database.sql.InsertSQL;
import org.example.database.sql.QuerySQL;
import org.example.database.sql.UpdateSQL;

import java.lang.reflect.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Repository<T, ID> implements InvocationHandler {

    private final Class<T> entityClass;
    private final Connection connection;

    public Repository(Class<T> entityClass) {
        this.entityClass = entityClass;
        this.connection = DataBase.conn;
    }

    public Repository(Class<T> entityClass, Connection connection) {
        this.entityClass = entityClass;
        this.connection = connection;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        switch (method.getName()) {
            case "save":
                return save((T) args[0]);
            case "findById":
                return findById((ID) args[0]);
            case "findAll":
                return findAll();
            case "delete":
                return delete((ID) args[0]);
            default:
                break;
        }

        if (method.isAnnotationPresent(Query.class)) {
            Query queryAnnotation = method.getAnnotation(Query.class);
            String query = new QuerySQL(method, args).get();

            if (queryAnnotation.entity()) {
                if (method.getReturnType().isAssignableFrom(List.class)) {
                    return executeQueryEntity(query, entityClass);
                } else {
                    return executeQuerySingleEntity(query, entityClass);
                }
            } else {
                return executeQuery(query);
            }
        } else if (method.isAnnotationPresent(Update.class)) {
            return this.execute(new QuerySQL(method, args).getUpdate());
        }

        return null;
    }

    public T findById(ID id) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        int countPk = 0;
        Table table = entityClass.getAnnotation(Table.class);
        sql.append(table.name()).append(" WHERE ");

        if (id instanceof List) {
            boolean first = true;

            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(PrimaryKey.class) && field.isAnnotationPresent(Column.class)) {
                    if (!first) {
                        sql.append(" AND ");
                    }

                    countPk++;
                    Column column = field.getAnnotation(Column.class);
                    sql.append(column.name()).append(" = ?");
                    first = false;
                }
            }
        } else {
            // Suporte para chave primária simples
            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(PrimaryKey.class) && field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    sql.append(column.name()).append(" = ?");
                    break;
                }
            }
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            if (id instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> keys = (List<Object>) id;

                if (keys.size() != countPk) {
                    throw new IllegalArgumentException("A quantidade de PK inválida.");
                }

                for (Object value : keys) {
                    preparedStatement.setObject(keys.indexOf(value) + 1, value);
                }

            } else {
                preparedStatement.setObject(1, id);
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    T obj = entityClass.getDeclaredConstructor().newInstance();

                    for (Field field : entityClass.getDeclaredFields()) {
                        if (field.isAnnotationPresent(Column.class)) {
                            Column column = field.getAnnotation(Column.class);
                            field.setAccessible(true);
                            Object value = resultSet.getObject(column.name());
                            field.set(obj, value);
                        }
                    }

                    return obj;
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    public List<T> findAll() {
        List<T> resultList = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM ");

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        Table table = entityClass.getAnnotation(Table.class);
        sql.append(table.name());

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql.toString())) {
            while (resultSet.next()) {
                T obj = entityClass.getDeclaredConstructor().newInstance();

                for (Field field : entityClass.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Column.class)) {
                        Column column = field.getAnnotation(Column.class);
                        field.setAccessible(true);

                        Object value = resultSet.getObject(column.name());
                        field.set(obj, value);
                    }
                }

                resultList.add(obj);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    private List<T> executeQueryEntity(String sql, Class<T> entityClass) {
        List<T> resultList = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                T entity = entityClass.getDeclaredConstructor().newInstance();

                for (Field field : entityClass.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Column.class)) {
                        Column column = field.getAnnotation(Column.class);
                        String columnName = column.name();
                        Object columnValue = resultSet.getObject(columnName);

                        field.setAccessible(true);
                        field.set(entity, columnValue);
                    }
                }

                resultList.add(entity);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    private T executeQuerySingleEntity(String sql, Class<T> entityClass) {
        T entity = null;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                entity = entityClass.getDeclaredConstructor().newInstance();

                for (Field field : entityClass.getDeclaredFields()) {
                    if (field.isAnnotationPresent(Column.class)) {
                        Column column = field.getAnnotation(Column.class);
                        String columnName = column.name();
                        Object columnValue = resultSet.getObject(columnName);

                        field.setAccessible(true);
                        field.set(entity, columnValue);
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entity;
    }

    private List<Map<String, Object>> executeQuery(String sql) {
        List<Map<String, Object>> resultList = new ArrayList<>();

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {

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

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    private T save(T entity) {
        try {
            if (!entityClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("A classe não possui a anotação @Table.");
            }

            List<Field> primaryKeyFields  = this.getPrimaryKeyField(entity);
            if (primaryKeyFields .isEmpty()) {
                throw new IllegalArgumentException("A classe não possui a anotação @PrimaryKey.");
            }

            StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ");
            Table table = entityClass.getAnnotation(Table.class);
            sql.append(table.name()).append(" WHERE ");

            for (int i = 0; i < primaryKeyFields.size(); i++) {
                Field primaryKeyField = primaryKeyFields.get(i);
                Column column = primaryKeyField.getAnnotation(Column.class);
                sql.append(column.name()).append(" = ?");

                if (i < primaryKeyFields.size() - 1) {
                    sql.append(" AND ");
                }
            }

            int count = 0;
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
                for (int index = 0; index < primaryKeyFields.size(); index++) {
                    Field primaryKeyField = primaryKeyFields.get(index);
                    primaryKeyField.setAccessible(true);
                    preparedStatement.setObject(index + 1, primaryKeyField.get(entity));
                }

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        count = resultSet.getInt(1);
                    }
                }
            }

            if (count > 0) {
                this.execute(new UpdateSQL(entity).get());
            } else {
                this.execute(new InsertSQL(entity).get());
            }

            return entity;
        } catch (IllegalAccessException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int delete(ID id) {
        StringBuilder sql = new StringBuilder("DELETE FROM ");

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        int countPk = 0;
        Table table = entityClass.getAnnotation(Table.class);
        sql.append(table.name()).append(" WHERE ");

        if (id instanceof List) {
            boolean first = true;

            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(PrimaryKey.class) && field.isAnnotationPresent(Column.class)) {
                    if (!first) {
                        sql.append(" AND ");
                    }

                    countPk++;
                    Column column = field.getAnnotation(Column.class);
                    sql.append(column.name()).append(" = ?");
                    first = false;
                }
            }
        } else {
            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(PrimaryKey.class) && field.isAnnotationPresent(Column.class)) {
                    Column column = field.getAnnotation(Column.class);
                    sql.append(column.name()).append(" = ?");
                    break;
                }
            }
        }

        int result = 0;
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            if (id instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> keys = (List<Object>) id;

                if (keys.size() != countPk) {
                    throw new IllegalArgumentException("A quantidade de PK inválida.");
                }

                for (Object value : keys) {
                    preparedStatement.setObject(keys.indexOf(value) + 1, value);
                }

            } else {
                preparedStatement.setObject(1, id);
            }

            result = preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private List<Field> getPrimaryKeyField(Object entity) {
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKeyFields.add(field);
            }
        }

        return primaryKeyFields;
    }

    private int execute(String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            return preparedStatement.executeUpdate();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T, ID> T createRepository(Class<?> repositoryInterface) {
        ParameterizedType parameterizedType = (ParameterizedType) repositoryInterface.getGenericInterfaces()[0];

        Class<T> entityClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
        Repository<T, ID> handler = new Repository<>(entityClass, DataBase.conn);

        return (T) Proxy.newProxyInstance(repositoryInterface.getClassLoader(),
                new Class<?>[]{repositoryInterface}, handler);
    }

    @SuppressWarnings("unchecked")
    public static <T, ID> T createRepository(Class<?> repositoryInterface, Connection connection) {
        ParameterizedType parameterizedType = (ParameterizedType) repositoryInterface.getGenericInterfaces()[0];

        Class<T> entityClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
        Repository<T, ID> handler = new Repository<>(entityClass, connection);

        return (T) Proxy.newProxyInstance(repositoryInterface.getClassLoader(),
                new Class<?>[]{repositoryInterface}, handler);
    }

}
