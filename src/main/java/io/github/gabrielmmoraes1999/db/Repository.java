package io.github.gabrielmmoraes1999.db;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.sql.*;
import io.github.gabrielmmoraes1999.db.util.*;

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
        String nameMethod = method.getName();

        if (method.isAnnotationPresent(Query.class)) {
            PreparedStatement preparedStatement = new QuerySQL(method, args).getPreparedStatement(connection);

            if (method.getReturnType().isAssignableFrom(entityClass)) {
                return QueryCustom.getEntity(preparedStatement, entityClass);
            } else if (method.getReturnType().isAssignableFrom(List.class)) {
                Class<?> classList = Function.getClassList(method);

                if (classList == entityClass) {
                    return QueryCustom.getEntityList(preparedStatement, entityClass);
                } else if (classList == Map.class) {
                    return QueryCustom.getMapList(preparedStatement);
                } else {
                    return QueryCustom.getObjectList(preparedStatement, classList);
                }
            } else if (method.getReturnType().isAssignableFrom(Map.class)) {
                return QueryCustom.getMap(preparedStatement);
            } else {
                return QueryCustom.getObject(preparedStatement, method.getReturnType());
            }
        } else if (method.isAnnotationPresent(Update.class)) {
            return new QuerySQL(method, args).update(connection);
        } else if (method.isAnnotationPresent(Delete.class)) {
            return new QuerySQL(method, args).delete(connection);
        }

        switch (nameMethod) {
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

        if (nameMethod.startsWith("findBy")) {
            return handleFindByMethod(method, args);
        } else if (nameMethod.startsWith("min") || nameMethod.startsWith("max") || nameMethod.startsWith("count")) {
            return minMaxCount(method, args);
        } else if (nameMethod.startsWith("next")) {
            return null;
        }

        throw new UnsupportedOperationException("Método não suportado: " + method.getName());
    }

    private Object handleFindByMethod(Method method, Object[] args) {
        String methodName = method.getName();
        Class<?> returnClass = method.getReturnType();

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        T resultObject = null;
        List<T> resultList = new ArrayList<>();
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

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    T entity = Function.getEntity(entityClass, resultSet);

                    if (returnClass == List.class) {
                        resultList.add(entity);
                    } else {
                        resultObject = entity;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (returnClass == List.class) {
            return resultList;
        } else {
            return resultObject;
        }
    }

    private Object minMaxCount(Method method, Object[] args) throws Throwable {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
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

    public List<T> findAll() {
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        List<T> entityList = new ArrayList<>();
        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        String sql = "SELECT * FROM " + table.name();

        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                entityList.add(Function.getEntity(entityClass, resultSet));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entityList;
    }

    public T findById(ID id) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ");

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
        }

        List<Field> primaryKeyFields  = Function.getPrimaryKeyField(entityClass);
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

        if (id instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> keys = (List<Object>) id;

            if (!Objects.equals(primaryKeyFields.size(), keys.size())) {
                throw new IllegalArgumentException("A quantidade de PK inválida.");
            }
        } else {
            if (!Objects.equals(primaryKeyFields.size(), 1)) {
                throw new IllegalArgumentException("A quantidade de PK inválida.");
            }
        }

        T result = null;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql.toString())) {
            if (id instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> keys = (List<Object>) id;
                Function.setParams(preparedStatement, keys);
            } else {
                Function.setPreparedStatement(preparedStatement, 1, id);
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    result = entityClass.getDeclaredConstructor().newInstance();

                    for (Field field : entityClass.getDeclaredFields()) {
                        if (field.isAnnotationPresent(Column.class)) {
                            Column column = Objects.requireNonNull(field.getAnnotation(Column.class));
                            field.setAccessible(true);
                            field.set(result, resultSet.getObject(column.name()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
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
        T result;

        try {
            if (!entityClass.isAnnotationPresent(Table.class)) {
                throw new IllegalArgumentException("A classe não possui a anotação @Table.");
            }

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
                    Function.setPreparedStatement(preparedStatement, index + 1, primaryKeyField.get(entity));
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

            result = entity;
        } catch (IllegalAccessException | SQLException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    private int delete(ID id) {
        StringBuilder sql = new StringBuilder("DELETE FROM ");

        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("A classe não possui a anotação @Table.");
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
                    throw new IllegalArgumentException("A quantidade de PK inválida.");
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

    private int execute(T entity, String sql, TypeSQL typeSQL)
            throws SQLException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class<?> clazz = entity.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Table table = Objects.requireNonNull(clazz.getAnnotation(Table.class));

        Map<String, Object> mapColumn = new LinkedHashMap<>();
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
                mapColumn.put(columnName, Function.convertDefaultValue(defaultValue, dataType));
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
                            Column column = Objects.requireNonNull(field.getAnnotation(Column.class));
                            field.setAccessible(true);

                            Object value = field.get(entity);
                            if (Objects.isNull(value)) {
                                value = mapColumn.get(column.name());
                            }

                            Function.setPreparedStatement(preparedStatement, index, value);
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
                            Column column = Objects.requireNonNull(field.getAnnotation(Column.class));
                            field.setAccessible(true);

                            Object value = field.get(entity);
                            if (Objects.isNull(value)) {
                                value = mapColumn.get(column.name());
                            }

                            Function.setPreparedStatement(preparedStatement, index, value);
                            index++;
                        }
                    }

                    for (Field field : primaryKeyList) {
                        field.setAccessible(true);
                        Function.setPreparedStatement(preparedStatement, index, field.get(entity));
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
