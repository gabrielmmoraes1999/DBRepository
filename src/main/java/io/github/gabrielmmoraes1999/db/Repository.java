package io.github.gabrielmmoraes1999.db;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.sql.InsertSQL;
import io.github.gabrielmmoraes1999.db.sql.QuerySQL;
import io.github.gabrielmmoraes1999.db.sql.UpdateSQL;
import io.github.gabrielmmoraes1999.db.util.Util;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

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

        if (method.isAnnotationPresent(Query.class)) {
            Query queryAnnotation = method.getAnnotation(Query.class);
            PreparedStatement preparedStatement = new QuerySQL(method, args).getPreparedStatement(connection);

            if (queryAnnotation.entity()) {
                if (method.getReturnType().isAssignableFrom(List.class)) {
                    return executeQueryEntity(preparedStatement, entityClass);
                } else {
                    return executeQuerySingleEntity(preparedStatement, entityClass);
                }
            } else {
                if (method.getReturnType().isAssignableFrom(List.class)) {
                    Class<?> classList = Util.getClassList(method);

                    if (classList == Map.class) {
                        return executeQueryListMap(preparedStatement);
                    } else {
                        return executeQuerySingleResultList(preparedStatement, classList);
                    }
                } else if (method.getReturnType().isAssignableFrom(Map.class)) {
                    return executeQueryMap(preparedStatement);
                } else {
                    return executeQuerySingleResult(preparedStatement, method.getReturnType());
                }
            }
        } else if (method.isAnnotationPresent(Update.class)) {
            return new QuerySQL(method, args).update(connection);
        } else if (method.isAnnotationPresent(Delete.class)) {
            return new QuerySQL(method, args).delete(connection);
        }

        switch (method.getName()) {
            case "insert":
                return insert((T) args[0]);
            case "update":
                return update((T) args[0]);
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

        T obj = null;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            if (id instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> keys = (List<Object>) id;

                if (keys.size() != countPk) {
                    throw new IllegalArgumentException("A quantidade de PK inválida.");
                }

                int index = 1;
                for (Object value : keys) {
                    preparedStatement.setObject(index, value);
                    index++;
                }

            } else {
                preparedStatement.setObject(1, id);
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    obj = entityClass.getDeclaredConstructor().newInstance();

                    for (Field field : entityClass.getDeclaredFields()) {
                        if (field.isAnnotationPresent(Column.class)) {
                            Column column = field.getAnnotation(Column.class);
                            field.setAccessible(true);
                            Object value = resultSet.getObject(column.name());
                            field.set(obj, value);
                        }
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return obj;
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

    private List<T> executeQueryEntity(PreparedStatement preparedStatement, Class<T> entityClass) {
        List<T> resultList = new ArrayList<>();

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
            }

            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return resultList;
    }

    private T executeQuerySingleEntity(PreparedStatement preparedStatement, Class<T> entityClass) {
        T entity = null;

        try {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
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
            }

            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entity;
    }

    private Map<String, Object> executeQueryMap(PreparedStatement preparedStatement) {
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

    private List<Map<String, Object>> executeQueryListMap(PreparedStatement preparedStatement) {
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

    private <R> R executeQuerySingleResult(PreparedStatement preparedStatement, Class<R> returnType) {
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

    private <R> List<R> executeQuerySingleResultList(PreparedStatement preparedStatement, Class<R> classList) {
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

    private Integer insert(T entity) {
        try {
            return this.execute(entity, new InsertSQL(entity).get(), TypeSQL.INSERT);
        } catch (IllegalAccessException | SQLException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private Integer update(T entity) {
        try {
            return this.execute(entity, new UpdateSQL(entity).get(), TypeSQL.UPDATE);
        } catch (IllegalAccessException | SQLException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private T save(T entity) {
        T obj;

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
                this.execute(entity, new UpdateSQL(entity).get(), TypeSQL.UPDATE);
            } else {
                this.execute(entity, new InsertSQL(entity).get(), TypeSQL.INSERT);
            }

            obj = entity;
        } catch (IllegalAccessException | SQLException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        return obj;
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
            @SuppressWarnings("unchecked")
            List<Object> keys = (List<Object>) id;

            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(PrimaryKey.class) && field.isAnnotationPresent(Column.class)) {
                    if (!first) {
                        sql.append(" AND ");
                    }

                    Column column = field.getAnnotation(Column.class);
                    if (keys.get(countPk) == null) {
                        sql.append(column.name()).append(" IS NULL");
                    } else {
                        sql.append(column.name()).append(" = ?");
                    }

                    first = false;
                    countPk++;
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

        int result;
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            if (id instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> keys = (List<Object>) id;

                if (keys.size() != countPk) {
                    throw new IllegalArgumentException("A quantidade de PK inválida.");
                }

                int index = 1;
                for (Object value : keys) {
                    if (value != null) {
                        preparedStatement.setObject(index, value);
                        index++;
                    }
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

    private int execute(T entity, String sql, TypeSQL typeSQL)
            throws SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class<?> clazz = entity.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Table table = clazz.getAnnotation(Table.class);

        Map<String, Object> mapColumn = new HashMap<>();
        ResultSet columns = connection.getMetaData().getColumns(
                null,
                null,
                table.name(),
                null);

        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME").trim();
            String defaultValue = columns.getString("COLUMN_DEF");
            int dataType = columns.getInt("DATA_TYPE");

            if (defaultValue != null) {
                mapColumn.put(columnName, Util.convertDefaultValue(defaultValue, dataType));
            }
        }

        int result;
        int index = 1;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            List<Field> primaryKeyList = new ArrayList<>();

            switch (typeSQL) {
                case INSERT:
                    for (Field field : fields) {
                        if (field.isAnnotationPresent(Column.class)) {
                            Column column = field.getAnnotation(Column.class);
                            field.setAccessible(true);

                            Object value = field.get(entity);
                            if (Objects.isNull(value)) {
                                value = mapColumn.get(column.name());
                            }

                            preparedStatement.setObject(index, value);
                            index++;
                        }
                    }
                    break;
                case UPDATE:
                    for (Field field : fields) {
                        if (field.isAnnotationPresent(PrimaryKey.class)) {
                            primaryKeyList.add(field);
                        }
                    }

                    for (Field field : fields) {
                        if (field.isAnnotationPresent(Column.class) && !primaryKeyList.contains(field)) {
                            Column column = field.getAnnotation(Column.class);
                            field.setAccessible(true);

                            Object value = field.get(entity);
                            if (Objects.isNull(value)) {
                                value = mapColumn.get(column.name());
                            }

                            preparedStatement.setObject(index, value);
                            index++;
                        }
                    }

                    for (Field field : primaryKeyList) {
                        field.setAccessible(true);
                        preparedStatement.setObject(index, field.get(entity));
                        index++;
                    }
                    break;
            }

            result = preparedStatement.executeUpdate();
        }

        return result;
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
