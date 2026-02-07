package io.github.gabrielmmoraes1999.db.sql;

import io.github.gabrielmmoraes1999.db.annotation.Column;
import io.github.gabrielmmoraes1999.db.annotation.JoinColumn;
import io.github.gabrielmmoraes1999.db.annotation.JoinColumns;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Objects;

public class SQLUtils {

    protected static int preparedStatement(String sql, Object entity, List<Field> fields, Connection connection) throws SQLException, IllegalAccessException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (Field field : fields) {
                field.setAccessible(true);
                SQLUtils.preparedStatement(
                        preparedStatement,
                        field.getType(),
                        fields.indexOf(field) + 1,
                        field.get(entity)
                );
            }
            return preparedStatement.executeUpdate();
        }
    }

    protected static PreparedStatement getPreparedStatement(String sql, Object entity, List<Field> fields, Connection connection) throws SQLException, IllegalAccessException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (Field field : fields) {
            field.setAccessible(true);
            SQLUtils.preparedStatement(preparedStatement, field.getType(), fields.indexOf(field) + 1, field.get(entity));
        }
        return preparedStatement;
    }

    private static void preparedStatement(PreparedStatement preparedStatement, Class<?> classType, int position, Object value) throws SQLException {
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
        } else if (classType == java.sql.Date.class) {
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

    public static void setPreparedStatement(PreparedStatement preparedStatement, int position, Object value) throws SQLException {
        if (Objects.isNull(value)) {
            preparedStatement.setObject(position, null);
        } else {
            Class<?> classType = value.getClass();

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

    protected static Class<?> getCollectionGenericType(Field field) {
        Type type = field.getGenericType();

        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Type[] args = pt.getActualTypeArguments();
            if (args.length > 0 && args[0] instanceof Class) {
                return (Class<?>) args[0];
            }
        }

        throw new RuntimeException("Não foi possível descobrir tipo genérico de " + field.getName());
    }

    protected static void copyJoinColumns(Object parent, Object child, Field oneToManyField) throws IllegalAccessException {
        JoinColumns joinColumns = oneToManyField.getAnnotation(JoinColumns.class);
        if (joinColumns == null) return;

        Class<?> parentClass = parent.getClass();
        Class<?> childClass = child.getClass();

        for (JoinColumn joinColumn : joinColumns.value()) {
            String referencedColumn = joinColumn.referencedColumnName();
            Field parentField = findFieldByColumn(parentClass, referencedColumn);
            Field childField = findFieldByColumn(childClass, joinColumn.name());

            if (parentField == null) {
                throw new RuntimeException("JoinColumn inválido: '" + joinColumn.name() + "' não existente na tabela pai.");
            }

            if (childField == null) {
                throw new RuntimeException("JoinColumn inválido: '" + joinColumn.name() + "' não existente na tabela filha.");
            }

            parentField.setAccessible(true);
            childField.setAccessible(true);

            Object value = parentField.get(parent);
            childField.set(child, value);
        }
    }

    protected static Field findFieldByColumn(Class<?> clazz, String columnName) {
        for (Field field : clazz.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null && column.name().equals(columnName)) {
                return field;
            }
        }
        return null;
    }

}
