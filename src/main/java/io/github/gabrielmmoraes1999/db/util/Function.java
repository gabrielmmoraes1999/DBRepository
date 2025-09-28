package io.github.gabrielmmoraes1999.db.util;

import io.github.gabrielmmoraes1999.db.TypeSQL;
import io.github.gabrielmmoraes1999.db.annotation.*;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Function {

    public static String getOrderBy(String methodName, Class<?> entityClass) {
        StringBuilder orderBy = new StringBuilder();

        if (methodName.contains("OrderBy")) {
            String[] part = methodName.split("OrderBy");

            Pattern pattern = Pattern.compile("([A-Z][a-zA-Z0-9]*?)(Asc|Desc)");
            Matcher matcher = pattern.matcher(part[1]);
            orderBy.append("ORDER BY ");

            while (matcher.find()) {
                String columnName = Function.getColumnName(entityClass, matcher.group(1));
                if (Objects.isNull(columnName)) {
                    throw new RuntimeException("Coluna não encontrada para o campo: " + matcher.group(1));
                }

                orderBy.append(columnName).append(" ");
                orderBy.append(matcher.group(2).toUpperCase()).append(", ");
            }

            orderBy.setLength(orderBy.length() - 2);
        }

        return orderBy.toString();
    }

    public static void setParams(PreparedStatement preparedStatement, List<Object> params) throws SQLException {
        for (int index = 0; index < params.size(); index++) {
            Function.setPreparedStatement(preparedStatement, index + 1, params.get(index));
        }
    }

    public static String getColumnName(Class<?> entityClass, String fieldName) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                Column columnAnnotation = field.getAnnotation(Column.class);
                return (columnAnnotation != null) ? columnAnnotation.name() : field.getName();
            }
        }

        throw new RuntimeException("Coluna não encontrada para o campo: " + fieldName);
    }

    public static <T> List<Field> getPrimaryKeyField(Class<T> entityClass) {
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKeyFields.add(field);
            }
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("The class does not have the annotation @PrimaryKey.");
        }

        return primaryKeyFields;
    }

    public static <T> List<Field> getPrimaryKeyField(T entity) {
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKeyFields.add(field);
            }
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("The class does not have the annotation @PrimaryKey.");
        }

        return primaryKeyFields;
    }

    public static Class<?> getClassList(Method method) {
        Class<?> listGenericType = null;
        Type returnType = method.getGenericReturnType();

        if (returnType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) returnType;
            Type type = parameterizedType.getActualTypeArguments()[0];

            if (type instanceof ParameterizedType) {
                ParameterizedType mapType = (ParameterizedType) type;
                listGenericType = (Class<?>) mapType.getRawType();
            } else {
                listGenericType = (Class<?>) type;
            }
        }

        return listGenericType;
    }

    public static Class<?> getWrapperClass(Class<?> primitiveType) {
        if (primitiveType == int.class) return Integer.class;
        if (primitiveType == long.class) return Long.class;
        if (primitiveType == double.class) return Double.class;
        if (primitiveType == float.class) return Float.class;
        if (primitiveType == boolean.class) return Boolean.class;
        if (primitiveType == char.class) return String.class;
        if (primitiveType == short.class) return Short.class;
        return primitiveType; // No caso de outros tipos
    }

    public static <T> T getEntity(Class<T> entityClass, Map<String, Object> rowData, Connection connection)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        T entity = entityClass.getDeclaredConstructor().newInstance();
        Table entityAnnotation = entityClass.getAnnotation(Table.class);

        // Primeiro popula os campos @Column
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                field.setAccessible(true);

                if (field.getType().isEnum()) {
                    assert column != null;
                    field.set(entity, field.getType().getEnumConstants()[(Integer) rowData.get(column.name())]);
                } else {
                    assert column != null;
                    field.set(entity, rowData.get(column.name()));
                }
            } else if (field.isAnnotationPresent(OneToMany.class) || field.isAnnotationPresent(OneToOne.class)) {
                // Depois trata os campos @OneToMany
                OrderBy orderBy = field.getAnnotation(OrderBy.class);
                String orderSql = orderBy != null ? " ORDER BY " + orderBy.value() : "";

                Class<?> childClass = null;

                if (field.isAnnotationPresent(OneToMany.class)) {
                    childClass = getGenericType(field);
                } else if (field.isAnnotationPresent(OneToOne.class)) {
                    childClass = field.getType();
                }

                if (childClass == null)
                    throw new IllegalArgumentException("The class does not have the type column.");

                if (!childClass.isAnnotationPresent(Table.class))
                    throw new IllegalArgumentException("The class does not have the annotation @Table.");

                String joinTable = childClass.getAnnotation(Table.class).name();
                Map<String, String> fkMap = Function.resolveJoinColumnsOrFKMeta(connection, entityAnnotation.name(), joinTable, field);

                if (fkMap.isEmpty())
                    throw new IllegalArgumentException("A classe não possui a anotação @PrimaryKey.");

                String whereClause = fkMap.keySet().stream()
                        .map(s -> s + " = ?")
                        .collect(Collectors.joining(" AND "));

                try (PreparedStatement stmt = connection.prepareStatement(String.format("SELECT * FROM %s WHERE %s %s", joinTable, whereClause, orderSql))) {
                    int index = 1;

                    for (String columnStr : fkMap.keySet()) {
                        for (Field field1 : entityClass.getDeclaredFields()) {
                            if (field1.isAnnotationPresent(Column.class)) {
                                Column column = Objects.requireNonNull(field1.getAnnotation(Column.class));

                                if (Objects.equals(column.name().toUpperCase(), columnStr)) {
                                    Function.setPreparedStatement(
                                            stmt, index, rowData.get(fkMap.get(columnStr))
                                    );

                                    index++;
                                }
                            }
                        }
                    }

                    List<Map<String, Object>> childList = Function.convertResultSetToMap(stmt.executeQuery());

                    field.setAccessible(true);
                    if (field.isAnnotationPresent(OneToMany.class)) {
                        Set<Object> collection = new LinkedHashSet<>();

                        for (Map<String, Object> childRow : childList) {
                            collection.add(getEntity(childClass, childRow, connection));
                        }

                        field.set(entity, collection);
                    } else if (field.isAnnotationPresent(OneToOne.class)) {
                        if (!childList.isEmpty()) {
                            field.set(entity, getEntity(childClass, childList.get(0), connection));
                        }
                    }

                }
            }
        }

        return entity;
    }


    public static void setPreparedStatement(PreparedStatement preparedStatement, int position, Object value) throws SQLException {
        if (Objects.isNull(value)) {
            preparedStatement.setObject(position, null);
        } else {
            Class<?> classType = Function.getWrapperClass(value.getClass());

            if (classType == Integer.class) {
                preparedStatement.setInt(position, (Integer) value);
            } else if (classType == Double.class) {
                preparedStatement.setDouble(position, (Double) value);
            } else if (classType == BigDecimal.class) {
                preparedStatement.setBigDecimal(position, (BigDecimal) value);
            } else if (classType == Boolean.class) {
                preparedStatement.setBoolean(position, (Boolean) value);
            } else if (classType == String.class) {
                preparedStatement.setString(position, (String) value);
            } else if (classType == Date.class) {
                preparedStatement.setDate(position, (Date) value);
            } else if (classType == Timestamp.class) {
                preparedStatement.setTimestamp(position, (Timestamp) value);
            } else if (classType == Time.class) {
                preparedStatement.setTime(position, (Time) value);
            } else if (classType == byte[].class) {
                preparedStatement.setBytes(position, (byte[]) value);
            } else if (classType.isEnum()) {
                preparedStatement.setInt(position, ((Enum<?>) value).ordinal());
            } else {
                preparedStatement.setObject(position, value);
            }
        }
    }

    public static List<String> extractColumns(String sql) {
        List<String> columns = new ArrayList<>();

        Pattern pattern = Pattern.compile("INSERT INTO\\s+\\w+\\s*\\(([^)]+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            String columnPart = matcher.group(1);
            String[] columnArray = columnPart.split(",");

            for (String col : columnArray) {
                columns.add(col.trim());
            }
        }

        return columns;
    }

    public static Class<?> getGenericType(Field field) {
        ParameterizedType type = (ParameterizedType) field.getGenericType();
        return (Class<?>) type.getActualTypeArguments()[0];
    }

    public static int executeSQL(String sql, List<Object> values, Connection connection) {
        int result;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            int index = 1;

            for (Object value : values) {
                Function.setPreparedStatement(preparedStatement, index, value);
                index++;
            }

            result = preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static int execute(Object entity, String sql, TypeSQL typeSQL, Connection connection) throws Exception {
        Class<?> clazz = entity.getClass();
        Field[] fields = clazz.getDeclaredFields();

        int result;
        int index = 1;

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            List<Field> primaryKeyList = new ArrayList<>();
            List<String> columnList = Function.extractColumns(sql);

            switch (typeSQL) {
                case INSERT:
                    for (Field field : fields) {
                        if (field.isAnnotationPresent(Column.class)) {
                            field.setAccessible(true);
                            Column column = Objects.requireNonNull(field.getAnnotation(Column.class));

                            if (columnList.contains(column.name())) {
                                Function.setPreparedStatement(preparedStatement, index, field.get(entity));
                                index++;
                            }
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
                            field.setAccessible(true);
                            Function.setPreparedStatement(preparedStatement, index, field.get(entity));
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

    static Map<String, String> resolveJoinColumnsOrFKMeta(Connection connection, String parentTable, String childTable, Field field) {
        Map<String, String> map = new LinkedHashMap<>();

        if (field.isAnnotationPresent(JoinColumns.class)) {
            JoinColumns joins = field.getAnnotation(JoinColumns.class);
            assert joins != null;
            for (JoinColumn jc : joins.value()) {
                map.put(jc.referencedColumnName().toUpperCase(), jc.name().toUpperCase());
            }
            return map;
        }

        if (field.isAnnotationPresent(JoinColumn.class)) {
            JoinColumn jc = field.getAnnotation(JoinColumn.class);
            assert jc != null;
            map.put(jc.referencedColumnName().toUpperCase(), jc.name().toUpperCase());
            return map;
        }

        map = getPrimaryKeys(connection, parentTable, childTable);

        if (map.isEmpty())
            map = getForeignKeys(connection, parentTable, childTable);

        return map;
    }

    private static Map<String, String> getPrimaryKeys(Connection conn, String parentTable, String childTable) {
        Map<String, String> foreignKeyMap = new LinkedHashMap<>();

        try (ResultSet rs = conn.getMetaData().getImportedKeys(null, null, childTable)) {
            while (rs.next()) {
                String pkTable = rs.getString("PKTABLE_NAME");
                String fkTable = rs.getString("FKTABLE_NAME");

                if (pkTable.equalsIgnoreCase(parentTable) && fkTable.equalsIgnoreCase(childTable)) {
                    String pkColumn = rs.getString("PKCOLUMN_NAME");
                    String fkColumn = rs.getString("FKCOLUMN_NAME");

                    foreignKeyMap.put(pkColumn.toUpperCase(), fkColumn.toUpperCase());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error reading foreign keys", e);
        }

        return foreignKeyMap;
    }

    private static Map<String, String> getForeignKeys(Connection conn, String parentTable, String childTable) {
        Map<String, String> foreignKeyMap = new LinkedHashMap<>();

        try (ResultSet rs = conn.getMetaData().getImportedKeys(null, null, parentTable)) {
            while (rs.next()) {
                String pkTable = rs.getString("PKTABLE_NAME");
                String fkTable = rs.getString("FKTABLE_NAME");

                if (fkTable.equalsIgnoreCase(parentTable) && pkTable.equalsIgnoreCase(childTable)) {
                    String pkColumn = rs.getString("PKCOLUMN_NAME");
                    String fkColumn = rs.getString("FKCOLUMN_NAME");

                    foreignKeyMap.put(pkColumn.toUpperCase(), fkColumn.toUpperCase());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error reading foreign keys", e);
        }

        return foreignKeyMap;
    }

    public static List<Map<String, Object>> convertResultSetToMap(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> columns = new LinkedHashMap<>();

            for (int i = 1; i <= columnCount; i++) {
                columns.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }

            rows.add(columns);
        }

        resultSet.close();
        return rows;
    }

}
