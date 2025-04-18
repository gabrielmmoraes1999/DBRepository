package io.github.gabrielmmoraes1999.db.util;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.PrimaryKey;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            Object value = params.get(index);
            int position = index + 1;

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
                } else {
                    preparedStatement.setObject(position, value);
                }
            }
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

    public static List<Field> getPrimaryKeyField(Object entity) {
        List<Field> primaryKeyFields = new ArrayList<>();

        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKeyFields.add(field);
            }
        }

        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("A classe não possui a anotação @PrimaryKey.");
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

    public static Object convertDefaultValue(String defaultValue, int dataType) {
        if (defaultValue == null) {
            return null; // Sem valor padrão
        }

        switch (dataType) {
            case Types.BOOLEAN:
            case Types.BIT:
                // O valor padrão no SQL pode ser 0/1 ou 'TRUE'/'FALSE'
                return Boolean.parseBoolean(defaultValue.equalsIgnoreCase("true") ? "true" : "false");

            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return Integer.parseInt(defaultValue);

            case Types.BIGINT:
                return Long.parseLong(defaultValue);

            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return Double.parseDouble(defaultValue);

            case Types.DECIMAL:
            case Types.NUMERIC:
                return new java.math.BigDecimal(defaultValue);

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                // Remover aspas simples ou duplas do valor padrão
                return defaultValue.replace("'", "").replace("\"", "");

            case Types.DATE:
                return Date.valueOf(defaultValue);

            case Types.TIMESTAMP:
                return Timestamp.valueOf(defaultValue);

            default:
                // Caso genérico, retornar o valor como String
                return defaultValue;
        }
    }

    public static <T> T getEntity(Class<T> entityClass, ResultSet resultSet)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        T entity = entityClass.getDeclaredConstructor().newInstance();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                field.setAccessible(true);

                assert column != null;
                field.set(entity, resultSet.getObject(column.name()));
            }
        }
        return entity;
    }
}
