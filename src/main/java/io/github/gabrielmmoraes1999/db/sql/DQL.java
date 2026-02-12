package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.*;
import io.github.gabrielmmoraes1999.db.core.EntityBuilder;
import io.github.gabrielmmoraes1999.db.parse.MethodNameParser;
import io.github.gabrielmmoraes1999.db.parse.ParsedQuery;
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
        if (!entityClass.isAnnotationPresent(Table.class)) {
            throw new IllegalArgumentException("The class does not have the annotation @Table.");
        }

        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        String sql = String.format("SELECT * FROM %s", table.name());
        return EntityBuilder.build(entityClass, sql, connection);
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

        Table table = entityClass.getAnnotation(Table.class);
        String tableName = table.name();

        StringJoiner selectClause = new StringJoiner(", ");
        StringJoiner whereClause = new StringJoiner(" AND ");
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) {
                continue;
            }
            Column column = field.getAnnotation(Column.class);

            if (field.isAnnotationPresent(PrimaryKey.class)) {
                whereClause.add(String.format("%s = ?", column.name()));
                primaryKeyFields.add(field);

                field.setAccessible(true);
                if (Objects.isNull(field.get(entity))) {
                    throw new IllegalArgumentException("Valor do @PrimaryKey é nulo");
                }
            }

            selectClause.add(column.name());
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("Classe sem @PrimaryKey");
        }

        String sql = String.format("SELECT %s FROM %s WHERE %s", selectClause, tableName, whereClause);
        List<T> result = EntityBuilder.build(entityClass, sql, connection);

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

        T resultClass = null;
        List<T> resultList = new ArrayList<>();
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();

        Table table = Objects.requireNonNull(entityClass.getAnnotation(Table.class));
        ParsedQuery query = MethodNameParser.parse(methodName);
        String sql = SqlRenderer.toSql(query, returnClass);

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                SQLUtils.setPreparedStatement(preparedStatement, i + 1, args[i]);
            }

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
                            JSONObject jsonObjectRow = new JSONObject();

                            for (int i = 1; i <= columnCount; i++) {
                                jsonObject.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                            }

                            jsonArray.put(jsonObjectRow);
                        }
                    }
                }
            } else if (returnClass.isAssignableFrom(List.class)) {
                resultList = EntityBuilder.build(entityClass, preparedStatement, connection);
            } else if (returnClass.isAssignableFrom(entityClass)) {
                List<T> entityList = EntityBuilder.build(entityClass, preparedStatement, connection);

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

}
