package io.github.gabrielmmoraes1999.db;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.sql.*;
import io.github.gabrielmmoraes1999.db.util.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

public class Repository<T, ID> implements InvocationHandler {

    private final Class<T> entityClass;
    private final Connection connectionGlobal;

    public Repository(Class<T> entityClass, Connection connectionGlobal) {
        this.entityClass = entityClass;
        this.connectionGlobal = connectionGlobal;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Connection connection;

        if (ConnectionPoolManager.isPresent()) {
            try {
                connection = ConnectionPoolManager.getConnection();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            connection = this.connectionGlobal;
        }

        boolean rollbackAutoCommit = false;
        String nameMethod = method.getName();
        Object returnObject;

        if (this.entityClass == null) {
            throw new IllegalArgumentException("The entity not found.");
        }

        try {
            if (method.isAnnotationPresent(Query.class)) {
                PreparedStatement preparedStatement = new QuerySQL(method, args).getPreparedStatement(connection);
                Class<?> returnType = method.getReturnType();

                if (returnType.isAssignableFrom(entityClass)) {
                    returnObject = QueryCustom.getEntity(preparedStatement, entityClass, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(List.class)) {
                    Class<?> classList = Function.getClassList(method);

                    if (classList.isAssignableFrom(entityClass)) {
                        returnObject = QueryCustom.getEntityList(preparedStatement, entityClass, connection);
                    } else if (classList.isAssignableFrom(Map.class)) {
                        returnObject = QueryCustom.getMapList(preparedStatement);
                    } else {
                        returnObject = QueryCustom.getObjectList(preparedStatement, classList);
                    }

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(Map.class)) {
                    returnObject = QueryCustom.getMap(preparedStatement);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(JSONObject.class)) {
                    returnObject = QueryCustom.getJsonObject(preparedStatement);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(JSONArray.class)) {
                    returnObject = QueryCustom.getJsonArray(preparedStatement);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAnnotationPresent(Table.class)) {
                    returnObject = QueryCustom.getEntity(preparedStatement, returnType, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else {
                    returnObject = QueryCustom.getObject(preparedStatement, returnType);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                }
            } else if (method.isAnnotationPresent(Update.class)) {
                returnObject = new QuerySQL(method, args).update(connection);

                DataBase.commit(connection);
                ConnectionPoolManager.closeConnection(connection);
                return returnObject;
            } else if (method.isAnnotationPresent(Delete.class)) {
                returnObject = new QuerySQL(method, args).delete(connection);

                DataBase.commit(connection);
                ConnectionPoolManager.closeConnection(connection);
                return returnObject;
            }

            switch (nameMethod) {
                case "insert":
                    returnObject = insert((T) args[0], connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "insertAll":
                    if (connection.getAutoCommit()) {
                        rollbackAutoCommit = true;
                        connection.setAutoCommit(false);
                    }

                    returnObject = insertAll((List<T>) args[0], connection);

                    if (rollbackAutoCommit)
                        connection.setAutoCommit(true);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "update":
                    returnObject = update((T) args[0], connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "save":
                    returnObject = save((T) args[0], connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "saveAll":
                    if (connection.getAutoCommit()) {
                        rollbackAutoCommit = true;
                        connection.setAutoCommit(false);
                    }

                    returnObject = saveAll((List<T>) args[0], connection);

                    if (rollbackAutoCommit)
                        connection.setAutoCommit(true);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "findById":
                    returnObject = findById((ID) args[0], connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "findAll":
                    returnObject = findAll(connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "deleteById":
                    returnObject = deleteById((ID) args[0], connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                default:
                    break;
            }

            if (nameMethod.startsWith("findBy")) {
                returnObject = handleFindByMethod(method, args, connection);

                DataBase.commit(connection);
                ConnectionPoolManager.closeConnection(connection);
                return returnObject;
            } else if (nameMethod.startsWith("min") || nameMethod.startsWith("max") || nameMethod.startsWith("count")) {
                returnObject = minMaxCount(method, args, connection);

                DataBase.commit(connection);
                ConnectionPoolManager.closeConnection(connection);
                return returnObject;
            }
        } catch (Exception ex) {
            try {
                DataBase.autoRollback(connection);
            } catch (SQLException ignore) {

            }

            if (rollbackAutoCommit)
                connection.setAutoCommit(true);
            ConnectionPoolManager.closeConnection(connection);
            throw new RuntimeException(ex);
        }

        throw new UnsupportedOperationException("Unsupported method: " + method.getName());
    }

    private Object handleFindByMethod(Method method, Object[] args, Connection connection) {
        String methodName = method.getName();
        Class<?> returnClass = method.getReturnType();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        T resultClass = null;
        List<T> resultList = new ArrayList<>();
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();

        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        FindBy findBy = new FindBy(methodName, args, entityClass);

        String sql = "SELECT * FROM " + String.join(
                " ",
                table.name(),
                findBy.getWhere(),
                Function.getOrderBy(methodName, entityClass)
        );

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            Function.setParams(preparedStatement, findBy.getParams());

            for (Map<String, Object> row : Function.convertResultSetToMap(preparedStatement.executeQuery())) {
                if (returnClass.isAssignableFrom(List.class)) {
                    resultList.add(Function.getEntity(entityClass, row, connection));
                } else if (returnClass.isAssignableFrom(entityClass)) {
                    resultClass = Function.getEntity(entityClass, row, connection);
                    break;
                } else if (returnClass.isAssignableFrom(JSONObject.class)) {
                    for (String columnName : row.keySet()) {
                        jsonObject.put(columnName, row.get(columnName));
                    }
                    break;
                } else if (returnClass.isAssignableFrom(JSONArray.class)) {
                    JSONObject jsonObjectRow = new JSONObject();

                    for (String columnName : row.keySet()) {
                        jsonObjectRow.put(columnName, row.get(columnName));
                    }

                    jsonArray.put(jsonObjectRow);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    private Object minMaxCount(Method method, Object[] args, Connection connection) throws Throwable {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        int functionNameSize;
        String columnName, function;
        String methodName = method.getName();
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        FindBy findBy = new FindBy(methodName, args, entityClass);

        if (methodName.startsWith("count")) {
            functionNameSize = 5;
        } else {
            functionNameSize = 3;
        }

        function = methodName.substring(0, functionNameSize).toUpperCase();
        if (methodName.contains("FindBy")) {
            columnName = Function.getColumnName(entityClass, methodName.substring(functionNameSize, methodName.indexOf("FindBy")));
        } else {
            columnName = Function.getColumnName(entityClass, methodName.substring(functionNameSize));
        }

        String sql = "SELECT " + function + "(" + columnName + ") FROM " + String.join(
                " ",
                table.name(),
                findBy.getWhere(),
                Function.getOrderBy(methodName, entityClass)
        );

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        Function.setParams(preparedStatement, findBy.getParams());
        return QueryCustom.getObject(preparedStatement, method.getReturnType());
    }

    public List<T> findAll(Connection connection) {
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

    public T findById(ID id, Connection connection) {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        List<Field> primaryKeyFields  = Function.getPrimaryKeyField(entityClass);
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(table.name()).append(" WHERE ");

        for (int i = 0; i < primaryKeyFields.size(); i++) {
            Field primaryKeyField = primaryKeyFields.get(i);
            Column column = Objects.requireNonNull(primaryKeyField.getAnnotation(Column.class));
            sql.append(column.name()).append(" = ?");

            if (i < primaryKeyFields.size() - 1) {
                sql.append(" AND ");
            }
        }

        if (id instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> keys = (List<Object>) id;

            if (!Objects.equals(primaryKeyFields.size(), keys.size())) {
                throw new IllegalArgumentException("The amount PK invalid.");
            }
        } else {
            if (!Objects.equals(primaryKeyFields.size(), 1)) {
                throw new IllegalArgumentException("The amount PK invalid.");
            }
        }

        T result = null;

        List<Map<String, Object>> childList;
        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
                if (id instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> keys = (List<Object>) id;
                    Function.setParams(preparedStatement, keys);
                } else {
                    Function.setPreparedStatement(preparedStatement, 1, id);
                }

                childList = Function.convertResultSetToMap(preparedStatement.executeQuery());
            }

            if (!childList.isEmpty()) {
                result = Function.getEntity(entityClass, childList.get(0), connection);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private Integer insert(T entity, Connection connection) {
        try {
            int register = Function.execute(entity, new InsertSQL(entity).get(), TypeSQL.INSERT, connection);
            SubClass.execute(entity, TypeSQL.INSERT, connection);
            return register;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Integer insertAll(List<T> entityList, Connection connection) {
        try {
            int register = 0;

            for (T entity : entityList) {
                Function.execute(entity, new InsertSQL(entity).get(), TypeSQL.INSERT, connection);
                SubClass.execute(entity, TypeSQL.INSERT, connection);
                register++;
            }

            return register;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Integer update(T entity, Connection connection) {
        try {
            int register = Function.execute(entity, new UpdateSQL(entity).get(), TypeSQL.UPDATE, connection);
            SubClass.execute(entity, TypeSQL.UPDATE, connection);
            return register;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T save(T entity, Connection connection) {
        T result;

        try {
            if (!entityClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("The class does not have the annotation @Table.");
            }

            List<Object> valuesPk = new ArrayList<>();
            List<Field> primaryKeyFields = Function.getPrimaryKeyField(entity);
            StringBuilder sql = new StringBuilder("SELECT COUNT(1) FROM ");
            Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
            sql.append(table.name()).append(" WHERE ");

            for (int i = 0; i < primaryKeyFields.size(); i++) {
                Field primaryKeyField = primaryKeyFields.get(i);
                Column column = Objects.requireNonNull(primaryKeyField.getAnnotation(Column.class));
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
                    valuesPk.add(primaryKeyField.get(entity));
                    Function.setPreparedStatement(preparedStatement, index + 1, primaryKeyField.get(entity));
                }

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        count = resultSet.getInt(1);
                    }
                }
            }

            if (count > 0) {
                Function.execute(entity, new UpdateSQL(entity).get(), TypeSQL.UPDATE, connection);
            } else {
                Function.execute(entity, new InsertSQL(entity).get(), TypeSQL.INSERT, connection);
            }

            SubClass.execute(entity, TypeSQL.UPDATE, connection);

            if (count > 0) {
                result = entity;
            } else {
                if (valuesPk.size() > 1) {
                    result = findById((ID) valuesPk, connection);
                } else {
                    result = findById((ID) valuesPk.get(0), connection);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private List<T> saveAll(List<T> entityList, Connection connection) {
        List<T> result = new ArrayList<>();

        for (T entity : entityList) {
            result.add(save(entity, connection));
        }

        return result;
    }

    private int deleteById(ID id, Connection connection) {
        StringBuilder sql = new StringBuilder("DELETE FROM ");

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        int countPk = 0;
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
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

                    Column column = Objects.requireNonNull(field.getAnnotation(Column.class));
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
                    Column column = Objects.requireNonNull(field.getAnnotation(Column.class));
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
                    throw new IllegalArgumentException("The amount PK invalid.");
                }

                int index = 1;
                for (Object value : keys) {
                    if (value != null) {
                        Function.setPreparedStatement(preparedStatement, index, value);
                        index++;
                    }
                }

            } else {
                Function.setPreparedStatement(preparedStatement, 1, id);
            }

            result = preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> T createRepository(Class<?> repositoryInterface) {
        ParameterizedType parameterizedType = (ParameterizedType) repositoryInterface.getGenericInterfaces()[0];
        Class<T> entityClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];

        return (T) Proxy.newProxyInstance(
                repositoryInterface.getClassLoader(),
                new Class<?>[]{repositoryInterface},
                new Repository<>(entityClass, DataBase.conn)
        );
    }

    @SuppressWarnings("unchecked")
    public static <T> T createRepository(Class<?> repositoryInterface, Connection connection) {
        ParameterizedType parameterizedType = (ParameterizedType) repositoryInterface.getGenericInterfaces()[0];
        Class<T> entityClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];

        return (T) Proxy.newProxyInstance(
                repositoryInterface.getClassLoader(),
                new Class<?>[]{repositoryInterface},
                new Repository<>(entityClass, connection)
        );
    }

}
