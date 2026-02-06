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
                    returnObject =  DML.insertCascade((T) args[0], connection);

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
                    returnObject = DML.updateCascade((T) args[0], connection);

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
                    returnObject = DQL.findById(entityClass, (ID) args[0], connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "findAll":
                    returnObject = DQL.findAll(entityClass, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                case "deleteById":
                    returnObject = DML.deleteById((ID) args[0], entityClass, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                default:
                    break;
            }

            if (nameMethod.startsWith("findBy")) {
                returnObject = DQL.handleMethod(entityClass, method, args, connection);

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
            throw ex;
        }

        throw new UnsupportedOperationException("Unsupported method: " + method.getName());
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

    private Integer insertAll(List<T> entityList, Connection connection) throws SQLException, IllegalAccessException {
        int result = 0;
        for (T entity : entityList) {
            result = result + DML.insertCascade(entity, connection);
        }
        return result;
    }

    private T save(T entity, Connection connection) throws Exception {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        int count = DML.updateCascade(entity, connection);

        if (count == 0) {
            DML.insertCascade(entity, connection);
        }

        return DQL.findById(entity, connection);
    }

    private List<T> saveAll(List<T> entityList, Connection connection) throws Exception {
        List<T> result = new ArrayList<>();

        for (T entity : entityList) {
            result.add(save(entity, connection));
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
