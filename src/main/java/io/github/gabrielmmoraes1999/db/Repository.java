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
            connection = ConnectionPoolManager.getConnection();
        } else {
            connection = this.connectionGlobal;
        }

        boolean rollbackAutoCommit = false;
        String nameMethod = method.getName();
        Object returnObject;

        try {
            if (this.entityClass == null) {
                throw new IllegalArgumentException("The entity not found.");
            }

            if (!entityClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("The class does not have the annotation @Table.");
            }

            if (method.isAnnotationPresent(Query.class)) {
                Class<?> returnType = method.getReturnType();

                if (returnType.isAssignableFrom(entityClass)) {
                    throw new IllegalArgumentException("The @Query annotation does not support entity return types.");
//                    returnObject = DQLCustom.getEntity(entityClass, method, args, connection);
//
//                    DataBase.commit(connection);
//                    ConnectionPoolManager.closeConnection(connection);
//                    return returnObject;
                } else if (returnType.isAssignableFrom(List.class)) {
                    Class<?> classList = Function.getClassList(method);

                    if (classList.isAssignableFrom(entityClass)) {
                        throw new IllegalArgumentException("The @Query annotation does not support entity return types.");
//                        returnObject = DQLCustom.getEntityList(entityClass, method, args, connection);
                    } else if (classList.isAssignableFrom(Map.class)) {
                        returnObject = DQLCustom.getMapList(method, args, connection);
                    } else {
                        returnObject = DQLCustom.getObjectList(classList, method, args, connection);
                    }

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(Map.class)) {
                    returnObject = DQLCustom.getMap(method, args, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(JSONObject.class)) {
                    returnObject = DQLCustom.getJsonObject(method, args, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else if (returnType.isAssignableFrom(JSONArray.class)) {
                    returnObject = DQLCustom.getJsonArray(method, args, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                } else {
                    returnObject = DQLCustom.getObject(returnType, method, args, connection);

                    DataBase.commit(connection);
                    ConnectionPoolManager.closeConnection(connection);
                    return returnObject;
                }
            } else if (method.isAnnotationPresent(Update.class)) {
                returnObject = DMLCustom.update(method, args, connection);

                DataBase.commit(connection);
                ConnectionPoolManager.closeConnection(connection);
                return returnObject;
            } else if (method.isAnnotationPresent(Delete.class)) {
                returnObject = DMLCustom.delete(method, args, connection);

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
            }
        } catch (Exception ex) {
            try {
                DataBase.autoRollback(connection);
            } catch (SQLException ignore) {

            }

            if (rollbackAutoCommit) {
                connection.setAutoCommit(true);
            }

            ConnectionPoolManager.closeConnection(connection);
            throw ex;
        }

        throw new UnsupportedOperationException("Unsupported method: " + method.getName());
    }

    private Integer insertAll(List<T> entityList, Connection connection) throws SQLException, IllegalAccessException {
        int result = 0;
        for (T entity : entityList) {
            result = result + DML.insertCascade(entity, connection);
        }
        return result;
    }

    private T save(T entity, Connection connection) throws Exception {
        if (DML.updateCascade(entity, connection) == 0) {
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
