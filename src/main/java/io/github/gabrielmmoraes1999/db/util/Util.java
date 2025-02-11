package io.github.gabrielmmoraes1999.db.util;

import io.github.gabrielmmoraes1999.db.annotation.Column;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

public class Util {

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
        if (primitiveType == char.class) return Character.class;
        if (primitiveType == byte.class) return Byte.class;
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

    public static String getColumnName(Class<?> entityClass, String fieldName) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                Column columnAnnotation = field.getAnnotation(Column.class);
                return (columnAnnotation != null) ? columnAnnotation.name() : field.getName();
            }
        }
        return null;
    }
}
